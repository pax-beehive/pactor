package core

import (
	"context"
	"time"
)

type ActorID string
type ThreadID string
type MessageID string
type MailboxItemID string

type Runtime interface {
	ListActors(context.Context, ActorFilter) ([]ActorSummary, error)
	GetActor(context.Context, ActorID) (ActorView, error)
	ListThreads(context.Context, ThreadFilter) ([]ThreadSummary, error)
	GetThread(context.Context, ThreadID) (ThreadView, error)
	ListMailbox(context.Context, ActorID, MailboxFilter) ([]MailboxItemView, error)
	PostMessage(context.Context, PostMessageInput) (PostMessageResult, error)
	CreateActor(context.Context, CreateActorInput) (ActorView, error)
	UpdateActor(context.Context, ActorID, ActorPatch) error
	UpdateThread(context.Context, ThreadID, ThreadPatch) error
	Subscribe(context.Context, EventCursor, EventFilter) (EventStream, error)
}

type WorkerRuntime interface {
	ClaimNext(context.Context, ActorID, time.Duration) (WorkItem, error)
	Complete(context.Context, MailboxItemID, WorkOutput) error
	Defer(context.Context, MailboxItemID, DeferInput) error
	Fail(context.Context, MailboxItemID, WorkError) error
}

type ActorSummary struct {
	ID              ActorID
	Alias           string
	Name            string
	Role            string
	Status          string
	Executor        string
	ApprovalPolicy  string
	ActiveSessionID string
	FilesystemRoot  string
	MailboxQueued   int
	MailboxRunning  int
}

type ActorView struct {
	ActorSummary
	Description string
	Routines    []RoutineView
}

type RoutineView struct {
	Name     string
	Every    time.Duration
	Order    int
	LastTick time.Time
	NextTick time.Time
}

type ThreadSummary struct {
	ID           ThreadID
	Subject      string
	Status       string
	Participants []string
	LastMessage  string
	UpdatedAt    time.Time
	Unread       int
}

type ThreadView struct {
	ThreadSummary
	Messages []MessageView
}

type MessageView struct {
	ID               MessageID
	ThreadID         ThreadID
	From             string
	To               []string
	Cc               []string
	Kind             string
	Body             string
	ReplyToMessageID MessageID
	CreatedAt        time.Time
}

type MailboxItemView struct {
	ID        MailboxItemID
	ActorID   ActorID
	MessageID MessageID
	ThreadID  ThreadID
	Subject   string
	From      string
	Priority  int
	Status    string
	Body      string
	CreatedAt time.Time
}

type ActorFilter struct{}

type CreateActorInput struct {
	ID             ActorID
	Alias          string
	Name           string
	Responsibility string
	FilesystemRoot string
	Routines       []RoutineInput
}

type RoutineInput struct {
	Name  string
	Every time.Duration
}

type ActorPatch struct {
	ActiveSessionID *string
	Status          string
}

type ThreadFilter struct {
	ActorID ActorID
	Status  string
}

type MailboxFilter struct {
	Status string
}

type PostMessageInput struct {
	ThreadID         ThreadID
	From             string
	To               []string
	Cc               []string
	Subject          string
	Body             string
	Kind             string
	Priority         int
	ReplyToMessageID MessageID
}

type PostMessageResult struct {
	ThreadID  ThreadID
	MessageID MessageID
	Enqueued  []MailboxItemView
}

type ThreadPatch struct {
	Status string
}

type EventCursor struct {
	After int64
}

type EventFilter struct {
	ThreadID ThreadID
	ActorID  ActorID
}

type EventStream interface {
	Events() <-chan EventView
	Close() error
}

type EventView struct {
	ID        int64
	Type      string
	ActorID   ActorID
	ThreadID  ThreadID
	MessageID MessageID
	At        time.Time
	Summary   string
}

type WorkItem struct {
	MailboxItemView
	Actor  ActorView
	Thread ThreadView
}

type WorkOutput struct {
	Body     string
	Status   string
	Priority int
}

type DeferInput struct {
	Reason     string
	NextWakeAt time.Time
}

type WorkError struct {
	Message string
}
