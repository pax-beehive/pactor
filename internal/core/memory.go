package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	StatusQueued  = "queued"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
	StatusOpen    = "open"
)

type SeedData struct {
	Actors []ActorView
}

func SeedDemoData() SeedData {
	return SeedData{Actors: []ActorView{
		{
			ActorSummary: ActorSummary{
				ID:             "act_001",
				Alias:          "design",
				Name:           "Design",
				Role:           "Product and interface design",
				Status:         "idle",
				Executor:       "codex",
				ApprovalPolicy: "yolo",
			},
			Description: "Turns loose goals into concrete actor-thread tasks.",
			Routines: []RoutineView{
				{Name: "stale_decision_scan", Every: 24 * time.Hour, Order: 10},
			},
		},
		{
			ActorSummary: ActorSummary{
				ID:             "act_002",
				Alias:          "backend",
				Name:           "Backend",
				Role:           "Go APIs, storage, tests",
				Status:         "idle",
				Executor:       "codex",
				ApprovalPolicy: "yolo",
			},
			Description: "Owns backend implementation threads and contract replies.",
		},
		{
			ActorSummary: ActorSummary{
				ID:             "act_003",
				Alias:          "frontend",
				Name:           "Frontend",
				Role:           "UI, routes, browser checks",
				Status:         "idle",
				Executor:       "codex",
				ApprovalPolicy: "yolo",
			},
			Description: "Owns user-facing surfaces and visual verification.",
		},
		{
			ActorSummary: ActorSummary{
				ID:             "act_004",
				Alias:          "ops",
				Name:           "Ops",
				Role:           "Local server health and evidence-backed bugs",
				Status:         "idle",
				Executor:       "codex",
				ApprovalPolicy: "yolo",
			},
			Description: "Runs routine checks and turns failures into thread messages.",
			Routines: []RoutineView{
				{Name: "health_check", Every: 30 * time.Second, Order: 10},
				{Name: "log_watch", Every: 10 * time.Second, Order: 20},
				{Name: "smoke_test", Every: 5 * time.Minute, Order: 30},
			},
		},
	}}
}

type MemoryRuntime struct {
	mu           sync.RWMutex
	nextActor    int
	nextThread   int
	nextMessage  int
	nextMailbox  int
	nextEvent    int64
	actors       map[ActorID]ActorView
	actorAliases map[string]ActorID
	threads      map[ThreadID]*threadRecord
	messages     map[MessageID]MessageView
	mailbox      map[MailboxItemID]MailboxItemView
	threadOrder  []ThreadID
	mailboxOrder []MailboxItemID
	events       []EventView
	sandboxRoot  string
}

type threadRecord struct {
	summary  ThreadSummary
	messages []MessageID
}

type MemorySnapshot struct {
	Version     int               `json:"version"`
	NextActor   int               `json:"nextActor"`
	NextThread  int               `json:"nextThread"`
	NextMessage int               `json:"nextMessage"`
	NextMailbox int               `json:"nextMailbox"`
	NextEvent   int64             `json:"nextEvent"`
	SandboxRoot string            `json:"sandboxRoot"`
	Actors      []ActorView       `json:"actors"`
	Threads     []ThreadSnapshot  `json:"threads"`
	Mailbox     []MailboxItemView `json:"mailbox"`
	Events      []EventView       `json:"events"`
}

type ThreadSnapshot struct {
	Summary  ThreadSummary `json:"summary"`
	Messages []MessageView `json:"messages"`
}

func NewMemoryRuntime(seed SeedData) *MemoryRuntime {
	return NewMemoryRuntimeWithSandboxRoot(seed, defaultSandboxRoot())
}

func NewMemoryRuntimeWithSandboxRoot(seed SeedData, sandboxRoot string) *MemoryRuntime {
	sandboxRoot = cleanFilesystemRoot(sandboxRoot)
	rt := &MemoryRuntime{
		nextThread:   100,
		nextMessage:  1000,
		nextMailbox:  500,
		actors:       make(map[ActorID]ActorView),
		actorAliases: make(map[string]ActorID),
		threads:      make(map[ThreadID]*threadRecord),
		messages:     make(map[MessageID]MessageView),
		mailbox:      make(map[MailboxItemID]MailboxItemView),
		sandboxRoot:  sandboxRoot,
	}
	for _, actor := range seed.Actors {
		actor = rt.prepareActor(actor)
		rt.storeActorLocked(actor)
	}
	rt.seedWelcomeThread()
	return rt
}

func NewMemoryRuntimeFromSnapshot(snapshot MemorySnapshot) *MemoryRuntime {
	sandboxRoot := cleanFilesystemRoot(snapshot.SandboxRoot)
	rt := &MemoryRuntime{
		nextActor:    snapshot.NextActor,
		nextThread:   snapshot.NextThread,
		nextMessage:  snapshot.NextMessage,
		nextMailbox:  snapshot.NextMailbox,
		nextEvent:    snapshot.NextEvent,
		actors:       make(map[ActorID]ActorView),
		actorAliases: make(map[string]ActorID),
		threads:      make(map[ThreadID]*threadRecord),
		messages:     make(map[MessageID]MessageView),
		mailbox:      make(map[MailboxItemID]MailboxItemView),
		sandboxRoot:  sandboxRoot,
		events:       append([]EventView(nil), snapshot.Events...),
	}
	for _, actor := range snapshot.Actors {
		actor.MailboxQueued = 0
		actor.MailboxRunning = 0
		actor = rt.prepareActor(actor)
		rt.storeActorLocked(actor)
	}
	for _, thread := range snapshot.Threads {
		record := &threadRecord{summary: thread.Summary}
		for _, message := range thread.Messages {
			rt.messages[message.ID] = message
			record.messages = append(record.messages, message.ID)
			rt.bumpMessageCounter(message.ID)
		}
		rt.threads[thread.Summary.ID] = record
		rt.threadOrder = append(rt.threadOrder, thread.Summary.ID)
		rt.bumpThreadCounter(thread.Summary.ID)
	}
	for _, item := range snapshot.Mailbox {
		rt.mailbox[item.ID] = item
		rt.mailboxOrder = append(rt.mailboxOrder, item.ID)
		rt.bumpMailboxCounter(item.ID)
	}
	for _, event := range snapshot.Events {
		if event.ID > rt.nextEvent {
			rt.nextEvent = event.ID
		}
	}
	if rt.nextThread < 100 {
		rt.nextThread = 100
	}
	if rt.nextMessage < 1000 {
		rt.nextMessage = 1000
	}
	if rt.nextMailbox < 500 {
		rt.nextMailbox = 500
	}
	return rt
}

func (rt *MemoryRuntime) Snapshot() MemorySnapshot {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	actors := make([]ActorView, 0, len(rt.actors))
	for _, actor := range rt.actors {
		actor.MailboxQueued = 0
		actor.MailboxRunning = 0
		actors = append(actors, actor)
	}
	sort.Slice(actors, func(i, j int) bool { return actors[i].ID < actors[j].ID })

	threads := make([]ThreadSnapshot, 0, len(rt.threadOrder))
	for _, threadID := range rt.threadOrder {
		record := rt.threads[threadID]
		thread := ThreadSnapshot{Summary: record.summary}
		for _, messageID := range record.messages {
			thread.Messages = append(thread.Messages, rt.messages[messageID])
		}
		threads = append(threads, thread)
	}

	mailbox := make([]MailboxItemView, 0, len(rt.mailboxOrder))
	for _, itemID := range rt.mailboxOrder {
		mailbox = append(mailbox, rt.mailbox[itemID])
	}

	return MemorySnapshot{
		Version:     1,
		NextActor:   rt.nextActor,
		NextThread:  rt.nextThread,
		NextMessage: rt.nextMessage,
		NextMailbox: rt.nextMailbox,
		NextEvent:   rt.nextEvent,
		SandboxRoot: rt.sandboxRoot,
		Actors:      actors,
		Threads:     threads,
		Mailbox:     mailbox,
		Events:      append([]EventView(nil), rt.events...),
	}
}

func (rt *MemoryRuntime) ListActors(_ context.Context, _ ActorFilter) ([]ActorSummary, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	out := make([]ActorSummary, 0, len(rt.actors))
	for _, actor := range rt.actors {
		summary := actor.ActorSummary
		for _, item := range rt.mailbox {
			if item.ActorID != actor.ID {
				continue
			}
			switch item.Status {
			case StatusQueued:
				summary.MailboxQueued++
			case StatusRunning:
				summary.MailboxRunning++
			}
		}
		out = append(out, summary)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Alias < out[j].Alias })
	return out, nil
}

func (rt *MemoryRuntime) GetActor(_ context.Context, actorID ActorID) (ActorView, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	resolvedID, err := rt.resolveActorIDLocked(actorID)
	if err != nil {
		return ActorView{}, err
	}
	actor, ok := rt.actors[resolvedID]
	if !ok {
		return ActorView{}, fmt.Errorf("actor %q not found", actorID)
	}
	return actor, nil
}

func (rt *MemoryRuntime) ListThreads(_ context.Context, filter ThreadFilter) ([]ThreadSummary, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	actorAddress := ""
	legacyActorAddress := ""
	if filter.ActorID != "" {
		actorID, err := rt.resolveActorIDLocked(filter.ActorID)
		if err != nil {
			return nil, err
		}
		actorAddress = rt.actorAddressLocked(actorID)
		legacyActorAddress = "actor:" + string(actorID)
	}
	out := make([]ThreadSummary, 0, len(rt.threads))
	for _, id := range rt.threadOrder {
		record := rt.threads[id]
		if filter.Status != "" && record.summary.Status != filter.Status {
			continue
		}
		if actorAddress != "" && !contains(record.summary.Participants, actorAddress) && !contains(record.summary.Participants, legacyActorAddress) {
			continue
		}
		out = append(out, record.summary)
	}
	return out, nil
}

func (rt *MemoryRuntime) GetThread(_ context.Context, threadID ThreadID) (ThreadView, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	record, ok := rt.threads[threadID]
	if !ok {
		return ThreadView{}, fmt.Errorf("thread %q not found", threadID)
	}
	view := ThreadView{ThreadSummary: record.summary}
	for _, id := range record.messages {
		view.Messages = append(view.Messages, rt.messages[id])
	}
	return view, nil
}

func (rt *MemoryRuntime) ListMailbox(_ context.Context, actorID ActorID, filter MailboxFilter) ([]MailboxItemView, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	resolvedID, err := rt.resolveActorIDLocked(actorID)
	if err != nil {
		return nil, err
	}
	out := make([]MailboxItemView, 0)
	for _, id := range rt.mailboxOrder {
		item := rt.mailbox[id]
		if item.ActorID != resolvedID {
			continue
		}
		if filter.Status != "" && item.Status != filter.Status {
			continue
		}
		out = append(out, item)
	}
	sortMailbox(out)
	return out, nil
}

func (rt *MemoryRuntime) PostMessage(_ context.Context, input PostMessageInput) (PostMessageResult, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	body := strings.TrimSpace(input.Body)
	if body == "" {
		return PostMessageResult{}, errors.New("message body is required")
	}
	kind := input.Kind
	if kind == "" {
		kind = "user_message"
	}
	priority := input.Priority
	if priority == 0 {
		priority = 50
	}
	to := rt.normalizeRecipientsLocked(input.To)
	cc := rt.normalizeRecipientsLocked(input.Cc)

	threadID := input.ThreadID
	if threadID == "" {
		threadID = rt.newThreadID()
		subject := strings.TrimSpace(input.Subject)
		if subject == "" {
			subject = summarize(body)
		}
		rt.threads[threadID] = &threadRecord{
			summary: ThreadSummary{
				ID:           threadID,
				Subject:      subject,
				Status:       StatusOpen,
				Participants: uniqueParticipants(input.From, to, cc),
				UpdatedAt:    time.Now(),
			},
		}
		rt.threadOrder = append([]ThreadID{threadID}, rt.threadOrder...)
	} else if _, ok := rt.threads[threadID]; !ok {
		return PostMessageResult{}, fmt.Errorf("thread %q not found", threadID)
	}

	messageID := rt.newMessageID()
	now := time.Now()
	message := MessageView{
		ID:               messageID,
		ThreadID:         threadID,
		From:             input.From,
		To:               to,
		Cc:               cc,
		Kind:             kind,
		Body:             body,
		ReplyToMessageID: input.ReplyToMessageID,
		CreatedAt:        now,
	}
	rt.messages[messageID] = message

	record := rt.threads[threadID]
	record.messages = append(record.messages, messageID)
	record.summary.LastMessage = body
	record.summary.UpdatedAt = now
	record.summary.Participants = mergeParticipants(record.summary.Participants, uniqueParticipants(input.From, to, cc))
	rt.bumpThread(threadID)

	result := PostMessageResult{ThreadID: threadID, MessageID: messageID}
	for _, actorID := range rt.actorRecipientsLocked(to, cc) {
		if _, ok := rt.actors[actorID]; !ok {
			continue
		}
		itemID := rt.newMailboxID()
		item := MailboxItemView{
			ID:        itemID,
			ActorID:   actorID,
			MessageID: messageID,
			ThreadID:  threadID,
			Subject:   record.summary.Subject,
			From:      input.From,
			Priority:  priority,
			Status:    StatusQueued,
			Body:      body,
			CreatedAt: now,
		}
		rt.mailbox[itemID] = item
		rt.mailboxOrder = append(rt.mailboxOrder, itemID)
		result.Enqueued = append(result.Enqueued, item)
	}
	rt.appendEvent("message.posted", "", threadID, messageID, "message posted")
	return result, nil
}

func (rt *MemoryRuntime) CreateActor(_ context.Context, input CreateActorInput) (ActorView, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	aliasInput := input.Alias
	if strings.TrimSpace(aliasInput) == "" {
		aliasInput = string(input.ID)
	}
	alias, err := normalizeActorAlias(aliasInput)
	if err != nil {
		return ActorView{}, err
	}
	actorID, err := rt.createActorIDLocked(input.ID)
	if err != nil {
		return ActorView{}, err
	}
	if _, exists := rt.actors[actorID]; exists {
		return ActorView{}, fmt.Errorf("actor %q already exists", actorID)
	}
	if existingID, exists := rt.actorAliases[alias]; exists {
		return ActorView{}, fmt.Errorf("actor alias %q already exists for %s", alias, existingID)
	}
	name := strings.TrimSpace(input.Name)
	if name == "" {
		name = alias
	}
	responsibility := strings.TrimSpace(input.Responsibility)
	if responsibility == "" {
		return ActorView{}, errors.New("actor responsibility is required")
	}
	filesystemRoot := rt.actorFilesystemRoot(alias, input.FilesystemRoot)
	if err := os.MkdirAll(filesystemRoot, 0o755); err != nil {
		return ActorView{}, fmt.Errorf("create actor filesystem root %q: %w", filesystemRoot, err)
	}
	routines := make([]RoutineView, 0, len(input.Routines))
	for idx, routine := range input.Routines {
		routineName := strings.TrimSpace(routine.Name)
		if routineName == "" {
			continue
		}
		every := routine.Every
		if every <= 0 {
			every = time.Hour
		}
		routines = append(routines, RoutineView{
			Name:  routineName,
			Every: every,
			Order: (idx + 1) * 10,
		})
	}

	actor := ActorView{
		ActorSummary: ActorSummary{
			ID:             actorID,
			Alias:          alias,
			Name:           name,
			Role:           responsibility,
			Status:         "idle",
			Executor:       "codex",
			ApprovalPolicy: "yolo",
			FilesystemRoot: filesystemRoot,
		},
		Description: responsibility,
		Routines:    routines,
	}
	rt.storeActorLocked(actor)
	rt.appendEvent("actor.created", actorID, "", "", "actor created")
	return actor, nil
}

func (rt *MemoryRuntime) actorFilesystemRoot(alias string, requested string) string {
	requested = strings.TrimSpace(requested)
	if requested == "" || strings.EqualFold(requested, "default") || strings.EqualFold(requested, "auto") || strings.EqualFold(requested, "new") {
		return filepath.Join(rt.sandboxRoot, alias)
	}
	return cleanFilesystemRoot(requested)
}

func defaultSandboxRoot() string {
	cwd, err := os.Getwd()
	if err != nil {
		return filepath.Join(".pactor", "sandboxes")
	}
	return filepath.Join(cwd, ".pactor", "sandboxes")
}

func cleanFilesystemRoot(root string) string {
	root = strings.TrimSpace(root)
	if root == "" {
		return defaultSandboxRoot()
	}
	cleaned, err := filepath.Abs(root)
	if err != nil {
		return filepath.Clean(root)
	}
	return cleaned
}

func (rt *MemoryRuntime) UpdateActor(_ context.Context, actorID ActorID, patch ActorPatch) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	resolvedID, err := rt.resolveActorIDLocked(actorID)
	if err != nil {
		return err
	}
	actor, ok := rt.actors[resolvedID]
	if !ok {
		return fmt.Errorf("actor %q not found", actorID)
	}
	if patch.ActiveSessionID != nil {
		actor.ActiveSessionID = strings.TrimSpace(*patch.ActiveSessionID)
	}
	if patch.Status != "" {
		actor.Status = patch.Status
	}
	rt.storeActorLocked(actor)
	rt.appendEvent("actor.updated", resolvedID, "", "", "actor updated")
	return nil
}

func (rt *MemoryRuntime) UpdateThread(_ context.Context, threadID ThreadID, patch ThreadPatch) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	record, ok := rt.threads[threadID]
	if !ok {
		return fmt.Errorf("thread %q not found", threadID)
	}
	if patch.Status != "" {
		record.summary.Status = patch.Status
		record.summary.UpdatedAt = time.Now()
		rt.appendEvent("thread.updated", "", threadID, "", "thread status updated")
	}
	return nil
}

func (rt *MemoryRuntime) Subscribe(ctx context.Context, cursor EventCursor, filter EventFilter) (EventStream, error) {
	ch := make(chan EventView, 32)
	rt.mu.RLock()
	filterActorID := filter.ActorID
	if filterActorID != "" {
		if resolvedID, err := rt.resolveActorIDLocked(filter.ActorID); err == nil {
			filterActorID = resolvedID
		}
	}
	for _, event := range rt.events {
		if event.ID <= cursor.After {
			continue
		}
		if filterActorID != "" && event.ActorID != filterActorID {
			continue
		}
		if filter.ThreadID != "" && event.ThreadID != filter.ThreadID {
			continue
		}
		ch <- event
	}
	rt.mu.RUnlock()
	close(ch)
	return staticEventStream{ctx: ctx, ch: ch}, nil
}

func (rt *MemoryRuntime) ClaimNext(_ context.Context, actorID ActorID, _ time.Duration) (WorkItem, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	resolvedID, err := rt.resolveActorIDLocked(actorID)
	if err != nil {
		return WorkItem{}, err
	}
	actor, ok := rt.actors[resolvedID]
	if !ok {
		return WorkItem{}, fmt.Errorf("actor %q not found", actorID)
	}
	var candidates []MailboxItemView
	for _, item := range rt.mailbox {
		if item.ActorID == resolvedID && item.Status == StatusQueued {
			candidates = append(candidates, item)
		}
	}
	if len(candidates) == 0 {
		return WorkItem{}, errors.New("mailbox is empty")
	}
	sortMailbox(candidates)
	item := candidates[0]
	item.Status = StatusRunning
	rt.mailbox[item.ID] = item
	actor.Status = StatusRunning
	rt.storeActorLocked(actor)
	thread, err := rt.threadViewLocked(item.ThreadID)
	if err != nil {
		return WorkItem{}, err
	}
	rt.appendEvent("mailbox.claimed", resolvedID, item.ThreadID, item.MessageID, "mailbox item claimed")
	return WorkItem{MailboxItemView: item, Actor: actor, Thread: thread}, nil
}

func (rt *MemoryRuntime) Complete(_ context.Context, itemID MailboxItemID, output WorkOutput) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	item, ok := rt.mailbox[itemID]
	if !ok {
		return fmt.Errorf("mailbox item %q not found", itemID)
	}
	actor := rt.actors[item.ActorID]
	item.Status = StatusDone
	rt.mailbox[item.ID] = item
	actor.Status = "idle"
	rt.storeActorLocked(actor)

	replyBody := strings.TrimSpace(output.Body)
	if replyBody == "" {
		replyBody = "Processed."
	}
	messageID := rt.newMessageID()
	now := time.Now()
	message := MessageView{
		ID:               messageID,
		ThreadID:         item.ThreadID,
		From:             rt.actorAddressLocked(item.ActorID),
		To:               []string{"user:todd"},
		Kind:             "actor_reply",
		Body:             replyBody,
		ReplyToMessageID: item.MessageID,
		CreatedAt:        now,
	}
	rt.messages[messageID] = message
	record := rt.threads[item.ThreadID]
	record.messages = append(record.messages, messageID)
	record.summary.LastMessage = replyBody
	record.summary.UpdatedAt = now
	record.summary.Participants = mergeParticipants(record.summary.Participants, []string{message.From})
	rt.bumpThread(item.ThreadID)
	rt.appendEvent("mailbox.completed", item.ActorID, item.ThreadID, messageID, "mailbox item completed")
	return nil
}

func (rt *MemoryRuntime) Defer(_ context.Context, itemID MailboxItemID, input DeferInput) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	item, ok := rt.mailbox[itemID]
	if !ok {
		return fmt.Errorf("mailbox item %q not found", itemID)
	}
	item.Status = "deferred"
	rt.mailbox[itemID] = item
	rt.appendEvent("mailbox.deferred", item.ActorID, item.ThreadID, item.MessageID, input.Reason)
	return nil
}

func (rt *MemoryRuntime) Fail(_ context.Context, itemID MailboxItemID, workErr WorkError) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	item, ok := rt.mailbox[itemID]
	if !ok {
		return fmt.Errorf("mailbox item %q not found", itemID)
	}
	item.Status = StatusFailed
	rt.mailbox[itemID] = item
	actor := rt.actors[item.ActorID]
	actor.Status = "idle"
	rt.storeActorLocked(actor)
	rt.appendEvent("mailbox.failed", item.ActorID, item.ThreadID, item.MessageID, workErr.Message)
	return nil
}

func (rt *MemoryRuntime) seedWelcomeThread() {
	_, _ = rt.PostMessage(context.Background(), PostMessageInput{
		From:     "user:todd",
		To:       []string{"actor:backend"},
		Cc:       []string{"actor:design"},
		Subject:  "Pactor mailbox runtime",
		Body:     "Design the first Pactor slice: Bubble Tea TUI, actor mailbox, threaded replies, and Codex-session-only actors.",
		Kind:     "task",
		Priority: 80,
	})
}

func (rt *MemoryRuntime) threadViewLocked(threadID ThreadID) (ThreadView, error) {
	record, ok := rt.threads[threadID]
	if !ok {
		return ThreadView{}, fmt.Errorf("thread %q not found", threadID)
	}
	view := ThreadView{ThreadSummary: record.summary}
	for _, id := range record.messages {
		view.Messages = append(view.Messages, rt.messages[id])
	}
	return view, nil
}

func (rt *MemoryRuntime) newThreadID() ThreadID {
	rt.nextThread++
	return ThreadID(fmt.Sprintf("th_%03d", rt.nextThread))
}

func (rt *MemoryRuntime) newMessageID() MessageID {
	rt.nextMessage++
	return MessageID(fmt.Sprintf("msg_%04d", rt.nextMessage))
}

func (rt *MemoryRuntime) newMailboxID() MailboxItemID {
	rt.nextMailbox++
	return MailboxItemID(fmt.Sprintf("mb_%04d", rt.nextMailbox))
}

func (rt *MemoryRuntime) newActorID() ActorID {
	for {
		rt.nextActor++
		id := ActorID(fmt.Sprintf("act_%03d", rt.nextActor))
		if _, exists := rt.actors[id]; !exists {
			return id
		}
	}
}

func (rt *MemoryRuntime) createActorIDLocked(requested ActorID) (ActorID, error) {
	if strings.TrimSpace(string(requested)) == "" {
		return rt.newActorID(), nil
	}
	actorID, err := normalizeActorID(requested)
	if err != nil {
		return "", err
	}
	rt.bumpActorCounter(actorID)
	return actorID, nil
}

func (rt *MemoryRuntime) prepareActor(actor ActorView) ActorView {
	if strings.TrimSpace(actor.Alias) == "" {
		alias, err := normalizeActorAlias(string(actor.ID))
		if err == nil {
			actor.Alias = alias
		}
	}
	if strings.TrimSpace(actor.Alias) == "" {
		actor.Alias = strings.ToLower(strings.TrimSpace(actor.Name))
	}
	if strings.TrimSpace(actor.FilesystemRoot) == "" {
		actor.FilesystemRoot = filepath.Join(rt.sandboxRoot, actor.Alias)
	} else {
		actor.FilesystemRoot = cleanFilesystemRoot(actor.FilesystemRoot)
	}
	rt.bumpActorCounter(actor.ID)
	return actor
}

func (rt *MemoryRuntime) storeActorLocked(actor ActorView) {
	rt.actors[actor.ID] = actor
	if actor.Alias != "" {
		rt.actorAliases[actor.Alias] = actor.ID
	}
	rt.bumpActorCounter(actor.ID)
}

func (rt *MemoryRuntime) resolveActorIDLocked(ref ActorID) (ActorID, error) {
	raw := strings.TrimSpace(string(ref))
	raw = strings.TrimPrefix(raw, "@")
	raw = strings.TrimPrefix(raw, "actor:")
	if raw == "" {
		return "", errors.New("actor alias is required")
	}
	if _, ok := rt.actors[ActorID(raw)]; ok {
		return ActorID(raw), nil
	}
	alias, err := normalizeActorAlias(raw)
	if err != nil {
		return "", err
	}
	if actorID, ok := rt.actorAliases[alias]; ok {
		return actorID, nil
	}
	return "", fmt.Errorf("actor %q not found", raw)
}

func (rt *MemoryRuntime) actorAddressLocked(actorID ActorID) string {
	actor, ok := rt.actors[actorID]
	if !ok || strings.TrimSpace(actor.Alias) == "" {
		return "actor:" + string(actorID)
	}
	return "actor:" + actor.Alias
}

func (rt *MemoryRuntime) normalizeRecipientsLocked(values []string) []string {
	out := normalizeRecipients(values)
	for i, value := range out {
		if !strings.HasPrefix(value, "actor:") {
			continue
		}
		actorID, err := rt.resolveActorIDLocked(ActorID(strings.TrimPrefix(value, "actor:")))
		if err != nil {
			continue
		}
		out[i] = rt.actorAddressLocked(actorID)
	}
	return out
}

func (rt *MemoryRuntime) actorRecipientsLocked(to, cc []string) []ActorID {
	seen := make(map[ActorID]bool)
	var out []ActorID
	for _, value := range append(normalizeRecipients(to), normalizeRecipients(cc)...) {
		if !strings.HasPrefix(value, "actor:") {
			continue
		}
		id, err := rt.resolveActorIDLocked(ActorID(strings.TrimPrefix(value, "actor:")))
		if err != nil || seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out
}

func (rt *MemoryRuntime) bumpActorCounter(id ActorID) {
	var n int
	if _, err := fmt.Sscanf(string(id), "act_%03d", &n); err == nil && n > rt.nextActor {
		rt.nextActor = n
	}
}

func (rt *MemoryRuntime) bumpThreadCounter(id ThreadID) {
	var n int
	if _, err := fmt.Sscanf(string(id), "th_%03d", &n); err == nil && n > rt.nextThread {
		rt.nextThread = n
	}
}

func (rt *MemoryRuntime) bumpMessageCounter(id MessageID) {
	var n int
	if _, err := fmt.Sscanf(string(id), "msg_%04d", &n); err == nil && n > rt.nextMessage {
		rt.nextMessage = n
	}
}

func (rt *MemoryRuntime) bumpMailboxCounter(id MailboxItemID) {
	var n int
	if _, err := fmt.Sscanf(string(id), "mb_%04d", &n); err == nil && n > rt.nextMailbox {
		rt.nextMailbox = n
	}
}

func (rt *MemoryRuntime) appendEvent(eventType string, actorID ActorID, threadID ThreadID, messageID MessageID, summary string) {
	rt.nextEvent++
	rt.events = append(rt.events, EventView{
		ID:        rt.nextEvent,
		Type:      eventType,
		ActorID:   actorID,
		ThreadID:  threadID,
		MessageID: messageID,
		At:        time.Now(),
		Summary:   summary,
	})
}

func (rt *MemoryRuntime) bumpThread(threadID ThreadID) {
	out := []ThreadID{threadID}
	for _, id := range rt.threadOrder {
		if id != threadID {
			out = append(out, id)
		}
	}
	rt.threadOrder = out
}

type staticEventStream struct {
	ctx context.Context
	ch  <-chan EventView
}

func (s staticEventStream) Events() <-chan EventView {
	return s.ch
}

func (s staticEventStream) Close() error {
	return s.ctx.Err()
}

func sortMailbox(items []MailboxItemView) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Priority == items[j].Priority {
			return items[i].CreatedAt.Before(items[j].CreatedAt)
		}
		return items[i].Priority > items[j].Priority
	})
}

func uniqueParticipants(from string, to, cc []string) []string {
	participants := []string{}
	if from != "" {
		participants = append(participants, from)
	}
	participants = append(participants, normalizeRecipients(to)...)
	participants = append(participants, normalizeRecipients(cc)...)
	return mergeParticipants(nil, participants)
}

func mergeParticipants(left, right []string) []string {
	seen := make(map[string]bool)
	out := make([]string, 0, len(left)+len(right))
	for _, value := range append(left, right...) {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizeRecipients(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		value = strings.TrimPrefix(value, "@")
		if value == "" {
			continue
		}
		if !strings.Contains(value, ":") {
			value = "actor:" + value
		}
		out = append(out, value)
	}
	return out
}

func contains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func summarize(body string) string {
	body = strings.TrimSpace(strings.ReplaceAll(body, "\n", " "))
	if len(body) <= 56 {
		return body
	}
	return body[:53] + "..."
}

func normalizeActorID(actorID ActorID) (ActorID, error) {
	raw := strings.TrimSpace(string(actorID))
	raw = strings.TrimPrefix(raw, "@")
	raw = strings.TrimPrefix(raw, "actor:")
	raw = strings.ToLower(raw)
	if raw == "" {
		return "", errors.New("actor id is required")
	}
	if err := validateActorToken(raw, "actor id"); err != nil {
		return "", err
	}
	return ActorID(raw), nil
}

func normalizeActorAlias(alias string) (string, error) {
	raw := strings.TrimSpace(alias)
	raw = strings.TrimPrefix(raw, "@")
	raw = strings.TrimPrefix(raw, "actor:")
	raw = strings.ToLower(raw)
	if raw == "" {
		return "", errors.New("actor alias is required")
	}
	if err := validateActorToken(raw, "actor alias"); err != nil {
		return "", err
	}
	return raw, nil
}

func validateActorToken(raw string, label string) error {
	for _, r := range raw {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		if r == '-' || r == '_' {
			continue
		}
		return fmt.Errorf("%s %q may only contain lowercase letters, numbers, hyphen, and underscore", label, raw)
	}
	return nil
}
