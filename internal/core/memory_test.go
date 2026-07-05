package core

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPostMessageCreatesThreadAndMailboxItems(t *testing.T) {
	ctx := context.Background()
	rt := NewMemoryRuntime(SeedDemoData())

	result, err := rt.PostMessage(ctx, PostMessageInput{
		From:     "user:todd",
		To:       []string{"@frontend"},
		Cc:       []string{"actor:ops"},
		Subject:  "Build actor mailbox UI",
		Body:     "Add a three-column Bubble Tea mailbox view.",
		Kind:     "task",
		Priority: 72,
	})
	if err != nil {
		t.Fatalf("PostMessage returned error: %v", err)
	}
	if result.ThreadID == "" || result.MessageID == "" {
		t.Fatalf("expected thread and message ids, got %#v", result)
	}
	if len(result.Enqueued) != 2 {
		t.Fatalf("expected frontend and ops mailbox items, got %d", len(result.Enqueued))
	}

	thread, err := rt.GetThread(ctx, result.ThreadID)
	if err != nil {
		t.Fatalf("GetThread returned error: %v", err)
	}
	if thread.Subject != "Build actor mailbox UI" {
		t.Fatalf("unexpected subject %q", thread.Subject)
	}
	if len(thread.Messages) != 1 {
		t.Fatalf("expected one message, got %d", len(thread.Messages))
	}
	message := thread.Messages[0]
	if message.To[0] != "actor:frontend" || message.Cc[0] != "actor:ops" {
		t.Fatalf("recipients were not normalized: to=%v cc=%v", message.To, message.Cc)
	}
	if !contains(thread.Participants, "actor:frontend") || !contains(thread.Participants, "actor:ops") {
		t.Fatalf("thread participants missing actor recipients: %v", thread.Participants)
	}

	frontendMailbox, err := rt.ListMailbox(ctx, "frontend", MailboxFilter{Status: StatusQueued})
	if err != nil {
		t.Fatalf("ListMailbox returned error: %v", err)
	}
	found := false
	for _, item := range frontendMailbox {
		if item.MessageID == result.MessageID {
			found = true
			if item.Priority != 72 || item.Status != StatusQueued {
				t.Fatalf("unexpected mailbox item: %#v", item)
			}
		}
	}
	if !found {
		t.Fatalf("frontend mailbox missing message %s", result.MessageID)
	}
}

func TestCreateActorAddsCodexActorWithRoutines(t *testing.T) {
	ctx := context.Background()
	sandboxRoot := t.TempDir()
	rt := NewMemoryRuntimeWithSandboxRoot(SeedDemoData(), sandboxRoot)

	actor, err := rt.CreateActor(ctx, CreateActorInput{
		Alias:          "@reviewer",
		Name:           "Reviewer",
		Responsibility: "Review code changes and report risks.",
		Routines: []RoutineInput{
			{Name: "daily_review", Every: 24 * time.Hour},
			{Name: "stale_thread_scan", Every: time.Hour},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor returned error: %v", err)
	}
	if actor.Alias != "reviewer" {
		t.Fatalf("expected normalized actor alias reviewer, got %q", actor.Alias)
	}
	if actor.ID == "" || actor.ID == "reviewer" || !strings.HasPrefix(string(actor.ID), "act_") {
		t.Fatalf("expected generated internal actor id, got %q", actor.ID)
	}
	if actor.Executor != "codex" || actor.Status != "idle" || actor.ApprovalPolicy != "yolo" {
		t.Fatalf("unexpected actor defaults: %#v", actor.ActorSummary)
	}
	expectedFilesystem := filepath.Join(sandboxRoot, "reviewer")
	if actor.FilesystemRoot != expectedFilesystem {
		t.Fatalf("expected default filesystem %q, got %q", expectedFilesystem, actor.FilesystemRoot)
	}
	if info, err := os.Stat(expectedFilesystem); err != nil || !info.IsDir() {
		t.Fatalf("expected filesystem root directory to exist, info=%v err=%v", info, err)
	}
	if actor.Role != "Review code changes and report risks." {
		t.Fatalf("unexpected role %q", actor.Role)
	}
	if len(actor.Routines) != 2 {
		t.Fatalf("expected two routines, got %d", len(actor.Routines))
	}
	if actor.Routines[0].Name != "daily_review" || actor.Routines[0].Every != 24*time.Hour || actor.Routines[0].Order != 10 {
		t.Fatalf("unexpected first routine: %#v", actor.Routines[0])
	}

	listed, err := rt.GetActor(ctx, "reviewer")
	if err != nil {
		t.Fatalf("GetActor returned error: %v", err)
	}
	if listed.Name != "Reviewer" {
		t.Fatalf("created actor was not retrievable: %#v", listed)
	}
	if listed.ID != actor.ID {
		t.Fatalf("alias lookup returned wrong actor id: got %q want %q", listed.ID, actor.ID)
	}
}

func TestCreateActorUsesExplicitFilesystemRoot(t *testing.T) {
	ctx := context.Background()
	rt := NewMemoryRuntimeWithSandboxRoot(SeedDemoData(), t.TempDir())
	filesystemRoot := filepath.Join(t.TempDir(), "custom-reviewer-root")

	actor, err := rt.CreateActor(ctx, CreateActorInput{
		Alias:          "reviewer",
		Name:           "Reviewer",
		Responsibility: "Review code changes.",
		FilesystemRoot: filesystemRoot,
	})
	if err != nil {
		t.Fatalf("CreateActor returned error: %v", err)
	}
	if actor.FilesystemRoot != filesystemRoot {
		t.Fatalf("expected explicit filesystem %q, got %q", filesystemRoot, actor.FilesystemRoot)
	}
	if info, err := os.Stat(filesystemRoot); err != nil || !info.IsDir() {
		t.Fatalf("expected explicit filesystem directory to exist, info=%v err=%v", info, err)
	}
}

func TestCreateActorRejectsDuplicateAndInvalidID(t *testing.T) {
	ctx := context.Background()
	rt := NewMemoryRuntimeWithSandboxRoot(SeedDemoData(), t.TempDir())

	if _, err := rt.CreateActor(ctx, CreateActorInput{
		Alias:          "backend",
		Name:           "Backend copy",
		Responsibility: "Duplicate.",
	}); err == nil {
		t.Fatal("expected duplicate actor alias to fail")
	}
	if _, err := rt.CreateActor(ctx, CreateActorInput{
		Alias:          "bad actor",
		Name:           "Bad",
		Responsibility: "Invalid id.",
	}); err == nil {
		t.Fatal("expected invalid actor id to fail")
	}
}

func TestActorAliasAddressEnqueuesInternalMailboxID(t *testing.T) {
	ctx := context.Background()
	rt := NewMemoryRuntimeWithSandboxRoot(SeedDemoData(), t.TempDir())
	actor, err := rt.CreateActor(ctx, CreateActorInput{
		Alias:          "looper",
		Name:           "Looper",
		Responsibility: "Runs loops.",
	})
	if err != nil {
		t.Fatalf("CreateActor returned error: %v", err)
	}
	result, err := rt.PostMessage(ctx, PostMessageInput{
		From: "user:todd",
		To:   []string{"@looper"},
		Body: "Use the alias address.",
	})
	if err != nil {
		t.Fatalf("PostMessage returned error: %v", err)
	}
	if len(result.Enqueued) != 1 || result.Enqueued[0].ActorID != actor.ID {
		t.Fatalf("expected mailbox to store internal id %s, got %#v", actor.ID, result.Enqueued)
	}
	thread, err := rt.GetThread(ctx, result.ThreadID)
	if err != nil {
		t.Fatalf("GetThread returned error: %v", err)
	}
	if thread.Messages[0].To[0] != "actor:looper" {
		t.Fatalf("expected message address to use alias, got %#v", thread.Messages[0].To)
	}
	if _, err := rt.ClaimNext(ctx, "looper", time.Minute); err != nil {
		t.Fatalf("ClaimNext by alias returned error: %v", err)
	}
}

func TestSnapshotRoundTripsRuntimeState(t *testing.T) {
	ctx := context.Background()
	rt := NewMemoryRuntimeWithSandboxRoot(SeedDemoData(), t.TempDir())
	posted, err := rt.PostMessage(ctx, PostMessageInput{
		From:     "user:todd",
		To:       []string{"@frontend"},
		Subject:  "Persist this",
		Body:     "Keep this message after reload.",
		Priority: 90,
	})
	if err != nil {
		t.Fatalf("PostMessage returned error: %v", err)
	}

	restored := NewMemoryRuntimeFromSnapshot(rt.Snapshot())
	thread, err := restored.GetThread(ctx, posted.ThreadID)
	if err != nil {
		t.Fatalf("GetThread after restore returned error: %v", err)
	}
	if thread.Subject != "Persist this" || len(thread.Messages) != 1 {
		t.Fatalf("unexpected restored thread: %#v", thread)
	}
	mailbox, err := restored.ListMailbox(ctx, "frontend", MailboxFilter{Status: StatusQueued})
	if err != nil {
		t.Fatalf("ListMailbox after restore returned error: %v", err)
	}
	if len(mailbox) == 0 || mailbox[0].MessageID != posted.MessageID {
		t.Fatalf("restored mailbox missing message %s: %#v", posted.MessageID, mailbox)
	}
	next, err := restored.PostMessage(ctx, PostMessageInput{
		From: "user:todd",
		To:   []string{"@frontend"},
		Body: "Next ids should not collide.",
	})
	if err != nil {
		t.Fatalf("PostMessage after restore returned error: %v", err)
	}
	if next.ThreadID == posted.ThreadID || next.MessageID == posted.MessageID {
		t.Fatalf("restored counters collided: first=%#v next=%#v", posted, next)
	}
}

func TestParseRoutineInputsRejectsInvalidShape(t *testing.T) {
	if _, err := ParseRoutineInputs("daily_review 24h"); err == nil {
		t.Fatal("expected invalid routine shape to fail")
	}
	routines, err := ParseRoutineInputs("daily_review every 24h, stale_scan every 1h")
	if err != nil {
		t.Fatalf("ParseRoutineInputs returned error: %v", err)
	}
	if len(routines) != 2 || routines[0].Name != "daily_review" || routines[0].Every != 24*time.Hour {
		t.Fatalf("unexpected routines: %#v", routines)
	}
	if routines, err := ParseRoutineInputs("none"); err != nil || len(routines) != 0 {
		t.Fatalf("expected none to produce no routines, got routines=%v err=%v", routines, err)
	}
}

func TestClaimNextUsesPriorityAndMarksActorRunning(t *testing.T) {
	ctx := context.Background()
	rt := NewMemoryRuntime(SeedDemoData())

	_, err := rt.PostMessage(ctx, PostMessageInput{
		From:     "user:todd",
		To:       []string{"@frontend"},
		Subject:  "Low priority",
		Body:     "This can wait.",
		Priority: 10,
	})
	if err != nil {
		t.Fatalf("PostMessage low priority returned error: %v", err)
	}
	high, err := rt.PostMessage(ctx, PostMessageInput{
		From:     "user:todd",
		To:       []string{"@frontend"},
		Subject:  "High priority",
		Body:     "Handle this first.",
		Priority: 90,
	})
	if err != nil {
		t.Fatalf("PostMessage high priority returned error: %v", err)
	}

	work, err := rt.ClaimNext(ctx, "frontend", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if work.MessageID != high.MessageID {
		t.Fatalf("expected high priority message %s, got %s", high.MessageID, work.MessageID)
	}
	if work.Status != StatusRunning {
		t.Fatalf("expected claimed item status running, got %q", work.Status)
	}

	actor, err := rt.GetActor(ctx, "frontend")
	if err != nil {
		t.Fatalf("GetActor returned error: %v", err)
	}
	if actor.Status != StatusRunning {
		t.Fatalf("expected actor running, got %q", actor.Status)
	}
}

func TestCompleteAddsActorReplyAndReturnsActorToIdle(t *testing.T) {
	ctx := context.Background()
	rt := NewMemoryRuntime(SeedDemoData())

	result, err := rt.PostMessage(ctx, PostMessageInput{
		From:     "user:todd",
		To:       []string{"@frontend"},
		Subject:  "Render thread",
		Body:     "Please render the active thread.",
		Priority: 100,
	})
	if err != nil {
		t.Fatalf("PostMessage returned error: %v", err)
	}
	work, err := rt.ClaimNext(ctx, "frontend", time.Minute)
	if err != nil {
		t.Fatalf("ClaimNext returned error: %v", err)
	}
	if err := rt.Complete(ctx, work.ID, WorkOutput{Body: "Thread renderer is ready."}); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}

	thread, err := rt.GetThread(ctx, result.ThreadID)
	if err != nil {
		t.Fatalf("GetThread returned error: %v", err)
	}
	if len(thread.Messages) != 2 {
		t.Fatalf("expected original message plus actor reply, got %d", len(thread.Messages))
	}
	reply := thread.Messages[1]
	if reply.Kind != "actor_reply" || reply.From != "actor:frontend" {
		t.Fatalf("unexpected reply metadata: %#v", reply)
	}
	if reply.ReplyToMessageID != result.MessageID {
		t.Fatalf("expected reply_to %s, got %s", result.MessageID, reply.ReplyToMessageID)
	}
	if !strings.Contains(reply.Body, "renderer is ready") {
		t.Fatalf("unexpected reply body %q", reply.Body)
	}

	actor, err := rt.GetActor(ctx, "frontend")
	if err != nil {
		t.Fatalf("GetActor returned error: %v", err)
	}
	if actor.Status != "idle" {
		t.Fatalf("expected actor idle after complete, got %q", actor.Status)
	}
}

func TestUpdateActorBindsTrimmedCodexSession(t *testing.T) {
	ctx := context.Background()
	rt := NewMemoryRuntime(SeedDemoData())

	sessionID := "  019f-session  "
	if err := rt.UpdateActor(ctx, "backend", ActorPatch{ActiveSessionID: &sessionID}); err != nil {
		t.Fatalf("UpdateActor returned error: %v", err)
	}
	actor, err := rt.GetActor(ctx, "backend")
	if err != nil {
		t.Fatalf("GetActor returned error: %v", err)
	}
	if actor.ActiveSessionID != "019f-session" {
		t.Fatalf("expected trimmed session id, got %q", actor.ActiveSessionID)
	}
}
