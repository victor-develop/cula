package claudecode

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	cula "github.com/git-hulk/cula/pkg"
)

// fakeCLIEnv makes the test binary re-exec itself as a stand-in for the
// `claude` CLI: it dumps fakeCLILines lines of stream-json and exits at once.
const fakeCLIEnv = "CULA_TEST_FAKE_CLAUDE"

// More lines than the session's event channel holds, so the stdout reader is
// still blocked mid-stream when the child process exits.
const fakeCLILines = 1200

// The line claude-code's own MCP client writes to stdout, once per
// unauthenticated MCP server, alongside the stream-json it is supposed to be
// the only thing on that pipe.
const strayLine = "Client.listTools() called but server does not advertise tools capability - returning empty list"

// Makes the fake CLI print one stray line between two events instead of the
// bulk stream.
const fakeStrayEnv = "CULA_TEST_FAKE_CLAUDE_STRAY"

func TestMain(m *testing.M) {
	if os.Getenv(fakeCLIEnv) == "" {
		os.Exit(m.Run())
	}
	w := bufio.NewWriter(os.Stdout)
	if os.Getenv(fakeStrayEnv) != "" {
		fmt.Fprintln(w, `{"a":1}`)
		fmt.Fprintln(w, strayLine)
		fmt.Fprintln(w, `{"b":2}`)
		_ = w.Flush()
		os.Exit(0)
	}
	for i := 0; i < fakeCLILines; i++ {
		fmt.Fprintln(w, `{"a":1}`)
	}
	_ = w.Flush()
	os.Exit(0)
}

// TestSpawnDrainsOutputBeforeReapingChild guards the ordering in spawnAndWait:
// cmd.Wait closes the pipes it created, so it must run only after the output
// readers have finished. Reaping first makes their last read fail with
// "file already closed", which both drops trailing output and emits a spurious
// error event on every completed turn.
func TestSpawnDrainsOutputBeforeReapingChild(t *testing.T) {
	rt := New(cula.Config{BinaryPath: os.Args[0]})
	sess, err := rt.SpawnSession(context.Background(), cula.SessionInput{
		Runtime: cula.RuntimeClaudeCode,
		Prompt:  "hello",
		Env:     []string{fakeCLIEnv + "=1"},
	})
	if err != nil {
		t.Fatalf("SpawnSession: %v", err)
	}
	t.Cleanup(func() { _ = sess.Cancel(context.Background()) })

	// Start consuming late so the reader fills the event channel and blocks,
	// leaving it mid-stream while the child exits.
	time.Sleep(150 * time.Millisecond)

	var lines int
	var errs []string
	deadline := time.After(30 * time.Second)
	for done := false; !done; {
		select {
		case ev, ok := <-sess.Events():
			if !ok {
				done = true
				break
			}
			switch ev.Type {
			case cula.EventError:
				errs = append(errs, ev.Error)
			case cula.EventRaw:
				lines++
			case cula.EventDone:
				done = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for the turn to finish")
		}
	}

	if len(errs) > 0 {
		t.Errorf("completed turn emitted %d error event(s): %s", len(errs), strings.Join(errs, "; "))
	}
	if lines != fakeCLILines {
		t.Errorf("got %d stdout events, want %d (trailing output was dropped)", lines, fakeCLILines)
	}
}

// TestStrayStdoutLineIsNotAnError covers a CLI printing something that is not
// one of its events. claude-code does it on every turn: its MCP client writes
// plain text to the same stdout as the stream-json protocol, once per
// unauthenticated MCP server.
//
// Reported as an error, each of those becomes a failure in front of the user
// for something that is not one — and one they cannot even identify, since the
// message carries the parse error rather than the line. The line belongs in the
// stream as raw output, which is what it is.
func TestStrayStdoutLineIsNotAnError(t *testing.T) {
	rt := New(cula.Config{BinaryPath: os.Args[0]})
	sess, err := rt.SpawnSession(context.Background(), cula.SessionInput{
		Runtime: cula.RuntimeClaudeCode,
		Prompt:  "hello",
		Env:     []string{fakeCLIEnv + "=1", fakeStrayEnv + "=1"},
	})
	if err != nil {
		t.Fatalf("SpawnSession: %v", err)
	}
	t.Cleanup(func() { _ = sess.Cancel(context.Background()) })

	var errs, raw []string
	deadline := time.After(30 * time.Second)
	for done := false; !done; {
		select {
		case ev, ok := <-sess.Events():
			if !ok {
				done = true
				break
			}
			switch ev.Type {
			case cula.EventError:
				errs = append(errs, ev.Error)
			case cula.EventRaw:
				raw = append(raw, ev.Text)
			case cula.EventDone:
				done = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for the turn to finish")
		}
	}

	if len(errs) > 0 {
		t.Errorf("a stray stdout line was reported as %d error(s): %s", len(errs), strings.Join(errs, "; "))
	}
	var found bool
	for _, text := range raw {
		if text == strayLine {
			found = true
		}
	}
	if !found {
		t.Errorf("the stray line was dropped instead of passed through as raw output; raw texts: %q", raw)
	}
}
