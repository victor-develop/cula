package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"

	iruntime "github.com/git-hulk/cula/internal/runtime"
	cula "github.com/git-hulk/cula/pkg"
)

type session struct {
	input cula.SessionInput

	cmd   *exec.Cmd
	stdin io.WriteCloser

	events chan cula.Event

	cancelCtx context.CancelFunc
	promptCh  chan string
	doneCh    chan struct{}
	doneOnce  sync.Once
	wg        sync.WaitGroup
	nextID    atomic.Int64
	threadID  string
	mu        sync.Mutex
	sessionID string
	state     cula.State
}

var _ cula.Session = (*session)(nil)

func newSession(ctx context.Context, rt *Runtime, input cula.SessionInput) (cula.Session, error) {
	input = iruntime.NormalizeInput(input)
	runCtx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(runCtx, iruntime.BinaryPath(rt.cfg, "codex"), "app-server", "--listen", "stdio://")
	cmd.Dir = input.WorkingDir
	cmd.Env = iruntime.CommandEnv(input, rt.cfg)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, err
	}

	s := &session{
		input:     input,
		cmd:       cmd,
		stdin:     stdin,
		events:    make(chan cula.Event, 1024),
		cancelCtx: cancel,
		promptCh:  make(chan string, 16),
		doneCh:    make(chan struct{}),
		sessionID: input.SessionID,
		state:     cula.StateRunning,
	}
	iruntime.Emit(s.events, s.doneCh, cula.Event{Type: cula.EventState, Runtime: cula.RuntimeCodex, SessionID: input.SessionID, State: cula.StateRunning})
	if err := s.start(ctx, stdout, stderr); err != nil {
		_ = cmd.Process.Kill()
		cancel()
		return nil, err
	}
	return s, nil
}

func (s *session) start(ctx context.Context, stdout io.Reader, stderr io.Reader) error {
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	if err := s.initialize(scanner); err != nil {
		return err
	}

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.readStdout(scanner)
	}()
	go func() {
		defer s.wg.Done()
		s.readStderr(stderr)
	}()
	go s.wait()
	go s.consumePrompts()

	if s.input.Prompt != "" {
		if err := s.Send(ctx, s.input.Prompt); err != nil {
			_ = s.Cancel(ctx)
			return err
		}
	}
	return nil
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
		s.mu.Lock()
		s.state = cula.StateCanceled
		s.mu.Unlock()
		iruntime.Emit(s.events, nil, cula.Event{Type: cula.EventState, Runtime: cula.RuntimeCodex, SessionID: s.sessionID, State: cula.StateCanceled})
		close(s.doneCh)
		s.cancelCtx()
		if s.cmd != nil && s.cmd.Process != nil {
			_ = s.cmd.Process.Signal(syscall.SIGTERM)
		}
	})
	return nil
}

func (s *session) nextRequestID() int64 {
	return s.nextID.Add(1)
}

func (s *session) initialize(scanner *bufio.Scanner) error {
	initID := s.nextRequestID()
	if err := iruntime.WriteJSONLine(s.stdin, map[string]any{
		"jsonrpc": "2.0",
		"id":      initID,
		"method":  "initialize",
		"params": map[string]any{
			"clientInfo": map[string]string{"name": "cula", "version": "0.1.0"},
		},
	}); err != nil {
		return fmt.Errorf("send initialize: %w", err)
	}
	if _, err := s.readJSONRPCResponse(scanner, initID, false); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}
	if err := iruntime.WriteJSONLine(s.stdin, map[string]any{
		"jsonrpc": "2.0",
		"method":  "initialized",
	}); err != nil {
		return fmt.Errorf("send initialized: %w", err)
	}

	params := map[string]any{
		"cwd":            s.input.WorkingDir,
		"approvalPolicy": string(s.input.Permission),
		"sandbox":        string(s.input.Sandbox),
	}
	if params["approvalPolicy"] == "" {
		params["approvalPolicy"] = "never"
	}
	if params["sandbox"] == "" {
		params["sandbox"] = "danger-full-access"
	}
	if s.input.Model != "" {
		params["model"] = s.input.Model
	}
	method := "thread/start"
	if s.input.SessionID != "" {
		method = "thread/resume"
		params["threadId"] = s.input.SessionID
	}
	threadID, err := s.startThread(scanner, method, params)
	resumed := err == nil && method == "thread/resume"
	if err != nil && method == "thread/resume" {
		threadID, err = s.startThread(scanner, "thread/start", params)
	}
	if err != nil {
		return err
	}
	s.threadID = threadID
	s.sessionID = threadID
	phase := cula.SessionStarted
	if resumed {
		phase = cula.SessionResumed
	}
	s.emitSession(phase, s.doneCh)
	return nil
}

// emitSession reports a session lifecycle change (started/resumed/closed) as an
// activity event carrying the phase and the resolved session ID. The closed
// phase is emitted with a nil done channel so teardown can deliver it after
// doneCh has already been closed.
func (s *session) emitSession(phase string, done <-chan struct{}) {
	iruntime.Emit(s.events, done, cula.Event{
		Type:      cula.EventActivity,
		Runtime:   cula.RuntimeCodex,
		SessionID: s.sessionID,
		Activity:  &cula.Activity{Type: cula.ActivitySession, Parameters: []string{phase, s.sessionID}},
	})
}

func (s *session) startThread(scanner *bufio.Scanner, method string, params map[string]any) (string, error) {
	reqID := s.nextRequestID()
	if err := iruntime.WriteJSONLine(s.stdin, map[string]any{
		"jsonrpc": "2.0",
		"id":      reqID,
		"method":  method,
		"params":  params,
	}); err != nil {
		return "", fmt.Errorf("send %s: %w", method, err)
	}
	result, err := s.readJSONRPCResponse(scanner, reqID, true)
	if err != nil {
		return "", fmt.Errorf("%s: %w", method, err)
	}
	var resp struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return "", fmt.Errorf("parse %s: %w", method, err)
	}
	if resp.Thread.ID == "" {
		return "", fmt.Errorf("%s returned empty thread id", method)
	}
	return resp.Thread.ID, nil
}

func (s *session) consumePrompts() {
	for {
		select {
		case <-s.doneCh:
			return
		case prompt := <-s.promptCh:
			turnID := s.nextRequestID()
			req := map[string]any{
				"jsonrpc": "2.0",
				"id":      turnID,
				"method":  "turn/start",
				"params": map[string]any{
					"threadId": s.threadID,
					"input": []map[string]string{
						{"type": "text", "text": prompt},
					},
				},
			}
			if err := iruntime.WriteJSONLine(s.stdin, req); err != nil {
				iruntime.Emit(s.events, s.doneCh, cula.Event{
					Type:      cula.EventError,
					Runtime:   cula.RuntimeCodex,
					SessionID: s.sessionID,
					Error:     fmt.Sprintf("send turn/start: %v", err),
				})
			}
		}
	}
}

func (s *session) readStdout(scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var raw json.RawMessage
		if err := json.Unmarshal(line, &raw); err != nil {
			iruntime.Emit(s.events, s.doneCh, cula.Event{Type: cula.EventError, Runtime: cula.RuntimeCodex, SessionID: s.sessionID, Error: fmt.Sprintf("decode stdout: %v", err)})
			continue
		}
		ev, ok := ParseEvent(raw)
		if !ok {
			continue
		}
		s.emitEvent(raw, ev)
	}
	if err := scanner.Err(); err != nil {
		iruntime.Emit(s.events, s.doneCh, cula.Event{Type: cula.EventError, Runtime: cula.RuntimeCodex, SessionID: s.sessionID, Error: fmt.Sprintf("read stdout: %v", err)})
	}
}

func (s *session) readStderr(r io.Reader) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		iruntime.Emit(s.events, s.doneCh, cula.Event{
			Type:      cula.EventStderr,
			Runtime:   cula.RuntimeCodex,
			SessionID: s.sessionID,
			Error:     scanner.Text(),
		})
	}
	if err := scanner.Err(); err != nil {
		iruntime.Emit(s.events, s.doneCh, cula.Event{
			Type:      cula.EventError,
			Runtime:   cula.RuntimeCodex,
			SessionID: s.sessionID,
			Error:     fmt.Sprintf("read stderr: %v", err),
		})
	}
}

func (s *session) readJSONRPCResponse(scanner *bufio.Scanner, requestID int64, forward bool) (json.RawMessage, error) {
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var msg struct {
			ID     *json.RawMessage `json:"id"`
			Result json.RawMessage  `json:"result"`
			Error  *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
			Method string `json:"method"`
		}
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}
		if msg.ID != nil {
			var id int64
			if json.Unmarshal(*msg.ID, &id) == nil && id == requestID {
				if msg.Error != nil {
					return nil, fmt.Errorf("JSON-RPC error %d: %s", msg.Error.Code, msg.Error.Message)
				}
				return msg.Result, nil
			}
		}
		if forward {
			var raw json.RawMessage
			if json.Unmarshal(line, &raw) == nil {
				if ev, ok := ParseEvent(raw); ok {
					s.emitEvent(raw, ev)
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("stdout closed before response %d", requestID)
}

func (s *session) emitEvent(raw json.RawMessage, ev cula.Event) {
	ev.Runtime = cula.RuntimeCodex
	ev.SessionID = s.sessionID
	if s.input.IncludeRaw {
		ev.Raw = raw
	}
	iruntime.Emit(s.events, s.doneCh, ev)
}

func (s *session) wait() {
	// Drain the output pipes before reaping the process — cmd.Wait closes them,
	// so waiting on it first makes the readers' final Read fail with
	// "file already closed" (see os/exec StdoutPipe docs).
	s.wg.Wait()
	err := s.cmd.Wait()
	code := iruntime.ExitCode(err)
	s.mu.Lock()
	canceled := s.state == cula.StateCanceled
	s.mu.Unlock()
	if !canceled {
		state := cula.StateCompleted
		if code != 0 {
			state = cula.StateFailed
		}
		s.mu.Lock()
		s.state = state
		s.mu.Unlock()
		iruntime.Emit(s.events, nil, cula.Event{
			Type:      cula.EventState,
			Runtime:   cula.RuntimeCodex,
			SessionID: s.sessionID,
			State:     state,
			ExitCode:  &code,
		})
	}
	s.emitSession(cula.SessionClosed, nil)
	s.doneOnce.Do(func() {
		close(s.doneCh)
	})
	close(s.events)
}
