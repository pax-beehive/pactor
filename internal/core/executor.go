package core

import (
	"context"
	"fmt"
)

type Executor interface {
	Execute(context.Context, WorkItem) (WorkOutput, error)
}

type PreviewCodexExecutor struct{}

func (PreviewCodexExecutor) Execute(_ context.Context, item WorkItem) (WorkOutput, error) {
	session := item.Actor.ActiveSessionID
	if session == "" {
		session = "unbound local preview"
	}
	body := fmt.Sprintf(
		"Codex session `%s` received `%s` from %s.\n\nThis preview executor marks the mailbox item as handled without calling the Codex CLI. Set `PACTOR_CODEX_CLI=1` to route `/run` through the local Codex CLI adapter.",
		session,
		item.Subject,
		item.From,
	)
	return WorkOutput{Body: body, Status: StatusDone, Priority: 50}, nil
}
