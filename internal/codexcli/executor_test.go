package codexcli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pax-beehive/pactor/internal/core"
)

func TestBuildPromptIncludesActorRoleAndThreadHistory(t *testing.T) {
	item := core.WorkItem{
		Actor: core.ActorView{
			ActorSummary: core.ActorSummary{
				ID:             "backend",
				Alias:          "backend",
				Role:           "Go APIs",
				ApprovalPolicy: "yolo",
				FilesystemRoot: "/tmp/pactor-backend",
			},
		},
		Thread: core.ThreadView{
			ThreadSummary: core.ThreadSummary{
				ID:      "th_123",
				Subject: "Add mailbox API",
			},
			Messages: []core.MessageView{
				{
					CreatedAt: time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC),
					From:      "user:todd",
					To:        []string{"actor:backend"},
					Body:      "Please add the mailbox API.",
				},
			},
		},
	}

	prompt := buildPrompt(item)
	for _, expected := range []string{
		"Pactor actor `backend`",
		"Internal actor id: backend",
		"Role: Go APIs",
		"Approval policy: yolo",
		"Filesystem root: /tmp/pactor-backend",
		"Thread: th_123 - Add mailbox API",
		"user:todd -> actor:backend",
		"Please add the mailbox API.",
	} {
		if !strings.Contains(prompt, expected) {
			t.Fatalf("prompt missing %q:\n%s", expected, prompt)
		}
	}
}

func TestWorkingDirPrefersActorFilesystemRoot(t *testing.T) {
	actorRoot := filepath.Join(t.TempDir(), "actor-root")
	fallbackRoot := filepath.Join(t.TempDir(), "fallback-root")
	executor := Executor{Cwd: fallbackRoot}

	cwd, err := executor.workingDir(core.WorkItem{
		Actor: core.ActorView{
			ActorSummary: core.ActorSummary{FilesystemRoot: actorRoot},
		},
	})
	if err != nil {
		t.Fatalf("workingDir returned error: %v", err)
	}
	if cwd != actorRoot {
		t.Fatalf("expected actor root %q, got %q", actorRoot, cwd)
	}
	if info, err := os.Stat(actorRoot); err != nil || !info.IsDir() {
		t.Fatalf("expected actor root directory to exist, info=%v err=%v", info, err)
	}
}

func TestApprovalArgsDefaultsYoloToBypassFlag(t *testing.T) {
	args := strings.Join(approvalArgs("yolo"), " ")
	if !strings.Contains(args, "--dangerously-bypass-approvals-and-sandbox") {
		t.Fatalf("expected yolo approval args to include bypass flag, got %q", args)
	}

	sandboxArgs := strings.Join(approvalArgs("sandbox-only"), " ")
	if strings.Contains(sandboxArgs, "--dangerously-bypass-approvals-and-sandbox") {
		t.Fatalf("sandbox-only args should not include bypass flag: %q", sandboxArgs)
	}
	if !strings.Contains(sandboxArgs, `sandbox_mode="workspace-write"`) {
		t.Fatalf("expected sandbox-only args to set workspace-write sandbox, got %q", sandboxArgs)
	}
}
