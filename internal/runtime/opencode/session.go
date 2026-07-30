package opencode

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"

	iruntime "github.com/git-hulk/cula/internal/runtime"
	cula "github.com/git-hulk/cula/pkg"
)

var _ cula.Session = (*session)(nil)

type session struct {
	mu        sync.Mutex
	runtime   *Runtime
	input     cula.SessionInput
	events    chan cula.Event
	sessionID string
	childCmd  *exec.Cmd
	promptCh  chan string
	doneCh    chan struct{}
	doneOnce  sync.Once
	ctx       context.Context
	cancelCtx context.CancelFunc
	parser    eventParser
}

func newSession(ctx context.Context, rt *Runtime, input cula.SessionInput) (cula.Session, error) {
	input = iruntime.NormalizeInput(input)
	runCtx, cancel := context.WithCancel(context.Background())
	s := &session{
		runtime:   rt,
		input:     input,
		events:    make(chan cula.Event, 1024),
		sessionID: input.SessionID,
		promptCh:  make(chan string, 16),
		doneCh:    make(chan struct{}),
		ctx:       runCtx,
		cancelCtx: cancel,
		parser:    eventParser{},
	}
	iruntime.Emit(s.events, s.doneCh, cula.Event{
		Type:      cula.EventState,
		Runtime:   cula.RuntimeOpenCode,
		SessionID: input.SessionID,
		State:     cula.StateRunning,
	})
	go s.consumePrompts()
	if input.Prompt != "" {
		if err := s.Send(ctx, input.Prompt); err != nil {
			_ = s.Cancel(ctx)
			return nil, err
		}
	}
	return s, nil
}

func (s *session) Send(ctx context.Context, prompt string) error {
	if prompt == "" {
		return nil
	}
	select {
	case s.promptCh <- prompt:
		return nil
	case <-s.doneCh:
		return iruntime.ErrSessionClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *session) Events() <-chan cula.Event {
	return s.events
}

func (s *session) Cancel(ctx context.Context) error {
	s.doneOnce.Do(func() {
		exit := -1
		iruntime.Emit(s.events, nil, cula.Event{
			Type:      cula.EventState,
			Runtime:   cula.RuntimeOpenCode,
			SessionID: s.sessionID,
			State:     cula.StateCanceled,
			ExitCode:  &exit,
		})
		close(s.doneCh)
		s.cancelCtx()
		s.mu.Lock()
		cmd := s.childCmd
		s.mu.Unlock()
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		close(s.events)
	})
	return nil
}

func (s *session) consumePrompts() {
	for {
		select {
		case prompt := <-s.promptCh:
			exitCode := s.spawnAndWait(prompt)
			if exitCode != 0 {
				exit := exitCode
				iruntime.Emit(s.events, s.doneCh, cula.Event{
					Type:      cula.EventState,
					Runtime:   cula.RuntimeOpenCode,
					SessionID: s.sessionID,
					State:     cula.StateFailed,
					ExitCode:  &exit,
				})
			}
		case <-s.doneCh:
			return
		}
	}
}

func (s *session) spawnAndWait(prompt string) int {
	args := s.buildArgs(s.sessionID, prompt)
	cmd := exec.CommandContext(s.ctx, iruntime.BinaryPath(s.runtime.cfg, "opencode"), args...)
	cmd.Dir = s.input.WorkingDir
	cmd.Env = iruntime.CommandEnv(s.input, s.runtime.cfg)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.emitError(fmt.Sprintf("stdout pipe: %v", err))
		return 1
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		s.emitError(fmt.Sprintf("stderr pipe: %v", err))
		return 1
	}
	if err := cmd.Start(); err != nil {
		s.emitError(fmt.Sprintf("start opencode: %v", err))
		return 1
	}

	s.mu.Lock()
	s.childCmd = cmd
	s.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		s.readStdout(stdout)
	}()
	go func() {
		defer wg.Done()
		s.readStderr(stderr)
	}()

	// Drain the output pipes BEFORE reaping the process: cmd.Wait closes the
	// pipes it created, and os/exec documents that calling Wait before all
	// reads have completed is incorrect. Waiting the other way round makes the
	// readers' final Read fail with "file already closed", which surfaces as a
	// spurious error event on every completed turn (and can drop trailing
	// output).
	wg.Wait()
	waitErr := cmd.Wait()

	s.mu.Lock()
	s.childCmd = nil
	s.mu.Unlock()

	exitCode := iruntime.ExitCode(waitErr)
	if exitCode == 0 {
		iruntime.Emit(s.events, s.doneCh, cula.Event{
			Type:      cula.EventDone,
			Runtime:   cula.RuntimeOpenCode,
			SessionID: s.sessionID,
		})
	}
	return exitCode
}

func (s *session) buildArgs(sessionID, prompt string) []string {
	args := []string{"run", "--format", "json", "--thinking"}
	if s.input.Permission != cula.PermissionNever {
		args = append(args, "--dangerously-skip-permissions")
	}
	if s.input.Model != "" {
		args = append(args, "--model", s.input.Model)
	}
	if sessionID != "" {
		args = append(args, "--session", sessionID)
	}
	return append(args, prompt)
}

func (s *session) readStdout(r io.Reader) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var raw json.RawMessage
		if err := json.Unmarshal(line, &raw); err != nil {
			s.emitError(fmt.Sprintf("decode stdout: %v", err))
			continue
		}
		if id := s.parser.captureSession(raw); id != "" {
			s.sessionID = id
		}
		if usage, ok := ParseTokenUsage(raw); ok {
			s.emitEvent(raw, usage)
		}
		ev, ok := ParseEvent(raw)
		if !ok {
			ev = cula.Event{Type: cula.EventRaw}
		}
		s.emitEvent(raw, ev)
	}
	if err := scanner.Err(); err != nil {
		s.emitError(fmt.Sprintf("read stdout: %v", err))
	}
}

func (s *session) readStderr(r io.Reader) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		iruntime.Emit(s.events, s.doneCh, cula.Event{
			Type:      cula.EventStderr,
			Runtime:   cula.RuntimeOpenCode,
			SessionID: s.sessionID,
			Error:     scanner.Text(),
		})
	}
	if err := scanner.Err(); err != nil {
		s.emitError(fmt.Sprintf("read stderr: %v", err))
	}
}

func (s *session) emitEvent(raw json.RawMessage, ev cula.Event) {
	ev.Runtime = cula.RuntimeOpenCode
	ev.SessionID = s.sessionID
	if s.input.IncludeRaw {
		ev.Raw = raw
	}
	iruntime.Emit(s.events, s.doneCh, ev)
}

func (s *session) emitError(message string) {
	iruntime.Emit(s.events, s.doneCh, cula.Event{
		Type:      cula.EventError,
		Runtime:   cula.RuntimeOpenCode,
		SessionID: s.sessionID,
		Error:     message,
	})
}
