package codexcli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/pax-beehive/pactor/internal/core"
)

type Executor struct {
	Bin string
	Cwd string
}

func (e Executor) Execute(ctx context.Context, item core.WorkItem) (core.WorkOutput, error) {
	bin := e.Bin
	if bin == "" {
		bin = "codex"
	}
	outFile, err := os.CreateTemp("", "pactor-codex-*.txt")
	if err != nil {
		return core.WorkOutput{}, err
	}
	outPath := outFile.Name()
	_ = outFile.Close()
	defer os.Remove(outPath)

	cwd, err := e.workingDir(item)
	if err != nil {
		return core.WorkOutput{}, err
	}
	prompt := buildPrompt(item)
	args := []string{}
	if strings.TrimSpace(item.Actor.ActiveSessionID) != "" {
		args = append(args, "exec", "resume")
		args = append(args, approvalArgs(item.Actor.ApprovalPolicy)...)
		args = append(args, "-o", outPath, "--skip-git-repo-check", item.Actor.ActiveSessionID, "-")
	} else {
		args = append(args, "exec")
		args = append(args, approvalArgs(item.Actor.ApprovalPolicy)...)
		args = append(args, "--skip-git-repo-check", "--color", "never", "-o", outPath, "-")
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = cwd
	cmd.Stdin = strings.NewReader(prompt)
	output, err := cmd.CombinedOutput()
	final, readErr := os.ReadFile(outPath)
	if readErr == nil && strings.TrimSpace(string(final)) != "" {
		return core.WorkOutput{Body: strings.TrimSpace(string(final)), Status: core.StatusDone}, err
	}
	if err != nil {
		return core.WorkOutput{}, fmt.Errorf("codex cli failed: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return core.WorkOutput{Body: strings.TrimSpace(string(output)), Status: core.StatusDone}, nil
}

func buildPrompt(item core.WorkItem) string {
	var b strings.Builder
	alias := item.Actor.Alias
	if alias == "" {
		alias = string(item.Actor.ID)
	}
	fmt.Fprintf(&b, "You are the Pactor actor `%s`.\n", alias)
	fmt.Fprintf(&b, "Internal actor id: %s\n", item.Actor.ID)
	fmt.Fprintf(&b, "Role: %s\n", item.Actor.Role)
	fmt.Fprintf(&b, "Approval policy: %s.\n", item.Actor.ApprovalPolicy)
	if strings.TrimSpace(item.Actor.FilesystemRoot) != "" {
		fmt.Fprintf(&b, "Filesystem root: %s\n", item.Actor.FilesystemRoot)
	}
	fmt.Fprintf(&b, "Thread: %s - %s\n\n", item.Thread.ID, item.Thread.Subject)
	b.WriteString("Thread history:\n")
	for _, message := range item.Thread.Messages {
		fmt.Fprintf(&b, "\n[%s] %s -> %s\n%s\n", message.CreatedAt.Format("15:04:05"), message.From, strings.Join(message.To, ","), message.Body)
	}
	b.WriteString("\nReply as this actor. Keep it concise and actionable.\n")
	return b.String()
}

func (e Executor) workingDir(item core.WorkItem) (string, error) {
	cwd := strings.TrimSpace(item.Actor.FilesystemRoot)
	if cwd == "" {
		cwd = strings.TrimSpace(e.Cwd)
	}
	if cwd == "" {
		return "", nil
	}
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		return "", fmt.Errorf("prepare actor filesystem root %q: %w", cwd, err)
	}
	return cwd, nil
}

func approvalArgs(policy string) []string {
	if strings.EqualFold(strings.TrimSpace(policy), "yolo") {
		return []string{"--dangerously-bypass-approvals-and-sandbox"}
	}
	return []string{
		"-c", `sandbox_mode="workspace-write"`,
		"-c", `approval_policy="never"`,
	}
}
