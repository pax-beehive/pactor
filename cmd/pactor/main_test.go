package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIActorMailboxFlowPersistsState(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state.json")

	stdout, stderr, code := runCLI("--state", state, "init")
	if code != 0 {
		t.Fatalf("init failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}

	stdout, stderr, code = runCLI(
		"--state", state,
		"actor", "create",
		"looper",
		"--name", "Looper",
		"--role", "Runs loop experiments",
		"--routine", "loop_once every 1m",
	)
	if code != 0 {
		t.Fatalf("actor create failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "created actor:looper") {
		t.Fatalf("unexpected create output: %q", stdout)
	}
	if !strings.Contains(stdout, "id: act_") {
		t.Fatalf("create output should include generated internal id: %q", stdout)
	}

	stdout, stderr, code = runCLI("--state", state, "actors")
	if code != 0 {
		t.Fatalf("actors failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "looper") {
		t.Fatalf("actors output did not persist looper: %q", stdout)
	}

	stdout, stderr, code = runCLI("--state", state, "send", "--to", "@looper", "Check the next mailbox item")
	if code != 0 {
		t.Fatalf("send failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "enqueued 1 mailbox") {
		t.Fatalf("unexpected send output: %q", stdout)
	}

	stdout, stderr, code = runCLI("--state", state, "mailbox", "looper")
	if code != 0 {
		t.Fatalf("mailbox failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "queued") || !strings.Contains(stdout, "Check the next mailbox item") {
		t.Fatalf("unexpected mailbox output: %q", stdout)
	}

	stdout, stderr, code = runCLI("--state", state, "run", "looper")
	if code != 0 {
		t.Fatalf("run failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "processed") || !strings.Contains(stdout, "preview executor") {
		t.Fatalf("unexpected run output: %q", stdout)
	}

	stdout, stderr, code = runCLI("--state", state, "mailbox", "looper", "--status", "done")
	if code != 0 {
		t.Fatalf("mailbox done failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "done") {
		t.Fatalf("expected done mailbox item, got: %q", stdout)
	}
}

func TestCLIStatePathAndJSON(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state.json")
	stdout, stderr, code := runCLI("--state", state, "state", "path")
	if code != 0 {
		t.Fatalf("state path failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if strings.TrimSpace(stdout) != state {
		t.Fatalf("expected state path %q, got %q", state, stdout)
	}

	stdout, stderr, code = runCLI("--state", state, "--json", "actors")
	if code != 0 {
		t.Fatalf("json actors failed code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	if !strings.Contains(stdout, `"Alias": "backend"`) {
		t.Fatalf("expected JSON actors, got %q", stdout)
	}
}

func runCLI(args ...string) (string, string, int) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run(args, &stdout, &stderr)
	return stdout.String(), stderr.String(), code
}
