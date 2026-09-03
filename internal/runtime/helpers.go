package runtime

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	cula "github.com/git-hulk/cula/pkg"
)

var ErrSessionClosed = errors.New("session is closed")

func NormalizeInput(input cula.SessionInput) cula.SessionInput {
	if input.WorkingDir == "" {
		if wd, err := os.Getwd(); err == nil {
			input.WorkingDir = wd
		}
	}
	return input
}

func BinaryPath(cfg cula.Config, fallback string) string {
	if cfg.BinaryPath != "" {
		return cfg.BinaryPath
	}
	return fallback
}

func CommandEnv(input cula.SessionInput, cfg cula.Config) []string {
	env := os.Environ()
	env = append(env, cfg.Env...)
	env = append(env, input.Env...)
	return env
}

// StrayOutput turns a stdout line that is not part of the runtime's event
// stream into an event, without calling it a failure.
//
// Not everything a CLI prints is an event. claude-code, for one, is run with
// --output-format stream-json, and its own MCP client writes plain text to the
// same stdout — "Client.listTools() called but server does not advertise tools
// capability - returning empty list", once per unauthenticated MCP server, on
// every turn.
//
// Reporting those as EventError puts a failure in front of the user for
// something that is not one, and does it without the evidence: the message
// carries the parse error, not the line that failed to parse, so the text is
// gone by the time anyone reads it. Consumers that render errors end up with a
// red block per stray line and no way to find out what it was.
//
// EventRaw already means "output we did not interpret", which is exactly what
// this is. The line travels in Text so a consumer that wants it can log it, and
// one that only renders errors and text stays quiet.
func StrayOutput(kind cula.RuntimeKind, sessionID string, line []byte) cula.Event {
	return cula.Event{
		Type:      cula.EventRaw,
		Runtime:   kind,
		SessionID: sessionID,
		Text:      string(line),
	}
}

func Emit(events chan<- cula.Event, done <-chan struct{}, ev cula.Event) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}
	if done != nil {
		select {
		case <-done:
			return false
		default:
		}
	}
	if done == nil {
		events <- ev
		return true
	}
	select {
	case events <- ev:
		return true
	case <-done:
		return false
	}
}

func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return 1
}

func LookupRuntime(binary, name string, kind cula.RuntimeKind, displayName string) cula.RuntimeInfo {
	info := cula.RuntimeInfo{
		Kind:       kind,
		Name:       displayName,
		AuthStatus: cula.AuthNotInstalled,
	}
	if binary == "" {
		binary = name
	}
	if path, err := exec.LookPath(binary); err == nil {
		info.Installed = true
		info.BinaryPath = path
		info.AuthStatus = cula.AuthUnknown
	} else if filepath.IsAbs(binary) {
		if st, statErr := os.Stat(binary); statErr == nil && !st.IsDir() {
			info.Installed = true
			info.BinaryPath = binary
			info.AuthStatus = cula.AuthUnknown
		}
	}
	return info
}

func Truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func StringMapSummary(input map[string]any) string {
	if len(input) == 0 {
		return ""
	}
	for _, key := range []string{"command", "cmd", "path", "file_path", "pattern", "query", "url"} {
		if v, ok := input[key]; ok {
			if s := fmt.Sprint(v); strings.TrimSpace(s) != "" {
				return Truncate(s, 80)
			}
		}
	}
	data, err := json.Marshal(input)
	if err != nil {
		return ""
	}
	return Truncate(string(data), 80)
}

func ParseArguments(args string) map[string]any {
	args = strings.TrimSpace(args)
	if args == "" {
		return nil
	}
	var input map[string]any
	if json.Unmarshal([]byte(args), &input) == nil {
		return input
	}
	return map[string]any{"arguments": args}
}

func WriteJSONLine(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}
