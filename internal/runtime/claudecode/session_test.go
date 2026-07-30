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

func TestMain(m *testing.M) {
	if os.Getenv(fakeCLIEnv) == "" {
		os.Exit(m.Run())
	}
	w := bufio.NewWriter(os.Stdout)
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
