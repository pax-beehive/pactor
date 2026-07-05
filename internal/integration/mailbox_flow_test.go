package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pax-beehive/pactor/internal/core"
)

func TestThreadedMailboxFlowThroughPreviewCodexExecutor(t *testing.T) {
	ctx := context.Background()
	rt := core.NewMemoryRuntime(core.SeedDemoData())
	executor := core.PreviewCodexExecutor{}

	sessionID := "codex-session-for-integration-test"
	if err := rt.UpdateActor(ctx, "backend", core.ActorPatch{ActiveSessionID: &sessionID}); err != nil {
		t.Fatalf("UpdateActor returned error: %v", err)
	}
	posted, err := rt.PostMessage(ctx, core.PostMessageInput{
		From:     "user:todd",
		To:       []string{"@backend"},
		Subject:  "Integration test thread",
		Body:     "Reply in the same email-style thread after handling this mailbox item.",
		Kind:     "task",
		Priority: 100,
	})
	if err != nil {
		t.Fatalf("PostMessage returned error: %v", err)
	}

	work, err := rt.ClaimNext(ctx, "backend", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if work.Thread.ID != posted.ThreadID {
		t.Fatalf("expected work to hydrate thread %s, got %s", posted.ThreadID, work.Thread.ID)
	}

	output, err := executor.Execute(ctx, work)
	if err != nil {
		t.Fatalf("PreviewCodexExecutor returned error: %v", err)
	}
	if !strings.Contains(output.Body, sessionID) {
		t.Fatalf("executor output did not include bound session id: %q", output.Body)
	}
	if err := rt.Complete(ctx, work.ID, output); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	thread, err := rt.GetThread(ctx, posted.ThreadID)
	if err != nil {
		t.Fatalf("GetThread returned error: %v", err)
	}
	if len(thread.Messages) != 2 {
		t.Fatalf("expected user message and actor reply, got %d", len(thread.Messages))
	}
	reply := thread.Messages[1]
	if reply.From != "actor:backend" {
		t.Fatalf("expected backend reply, got %q", reply.From)
	}
	if reply.ReplyToMessageID != posted.MessageID {
		t.Fatalf("expected reply to original message %s, got %s", posted.MessageID, reply.ReplyToMessageID)
	}
	if !strings.Contains(reply.Body, "Codex session") {
		t.Fatalf("unexpected reply body %q", reply.Body)
	}
}
