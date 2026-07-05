package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pax-beehive/pactor/internal/codexcli"
	"github.com/pax-beehive/pactor/internal/core"
)

type app struct {
	statePath string
	jsonOut   bool
	out       io.Writer
	errOut    io.Writer
}

type stringList []string

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	application, rest, err := parseGlobalFlags(args, stdout, stderr)
	if err != nil {
		return 2
	}
	if len(rest) == 0 {
		application.printHelp()
		return 0
	}
	if err := application.dispatch(rest); err != nil {
		fmt.Fprintf(stderr, "pactor: %v\n", err)
		return 1
	}
	return 0
}

func parseGlobalFlags(args []string, stdout, stderr io.Writer) (app, []string, error) {
	flags := flag.NewFlagSet("pactor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	application := app{statePath: defaultStatePath(), out: stdout, errOut: stderr}
	flags.StringVar(&application.statePath, "state", application.statePath, "state file path")
	flags.BoolVar(&application.jsonOut, "json", false, "print JSON output")
	if err := flags.Parse(args); err != nil {
		return app{}, nil, err
	}
	return application, flags.Args(), nil
}

func (a app) dispatch(args []string) error {
	switch args[0] {
	case "help", "-h", "--help":
		a.printHelp()
		return nil
	case "init":
		return a.cmdInit(args[1:])
	case "state":
		return a.cmdState(args[1:])
	case "actors":
		return a.cmdActors(args[1:])
	case "actor":
		return a.cmdActor(args[1:])
	case "mailbox":
		return a.cmdMailbox(args[1:])
	case "send":
		return a.cmdSend(args[1:])
	case "reply":
		return a.cmdReply(args[1:])
	case "threads":
		return a.cmdThreads(args[1:])
	case "thread":
		return a.cmdThread(args[1:])
	case "session":
		return a.cmdSession(args[1:])
	case "run":
		return a.cmdRun(args[1:])
	case "close":
		return a.cmdClose(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func (a app) cmdInit(args []string) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	flags.SetOutput(a.errOut)
	force := flags.Bool("force", false, "overwrite existing state")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: pactor init [--force]")
	}
	if _, err := os.Stat(a.statePath); err == nil && !*force {
		return fmt.Errorf("state already exists at %s", a.statePath)
	}
	rt := core.NewMemoryRuntimeWithSandboxRoot(core.SeedDemoData(), a.sandboxRoot())
	if err := a.save(rt); err != nil {
		return err
	}
	fmt.Fprintf(a.out, "initialized %s\n", a.statePath)
	return nil
}

func (a app) cmdState(args []string) error {
	if len(args) == 0 || args[0] == "path" {
		fmt.Fprintln(a.out, a.statePath)
		return nil
	}
	return errors.New("usage: pactor state path")
}

func (a app) cmdActors(args []string) error {
	if len(args) != 0 {
		return errors.New("usage: pactor actors")
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	actors, err := rt.ListActors(context.Background(), core.ActorFilter{})
	if err != nil {
		return err
	}
	if a.jsonOut {
		return writeJSON(a.out, actors)
	}
	tw := tabwriter.NewWriter(a.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ALIAS\tID\tNAME\tSTATUS\tQ\tRUNNING\tSESSION")
	for _, actor := range actors {
		session := actor.ActiveSessionID
		if session == "" {
			session = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\t%s\n", actor.Alias, actor.ID, actor.Name, actor.Status, actor.MailboxQueued, actor.MailboxRunning, session)
	}
	return tw.Flush()
}

func (a app) cmdActor(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: pactor actor show <alias> | pactor actor create <alias> --role ROLE [--name NAME] [--fs PATH] [--routine 'name every 1m']")
	}
	switch args[0] {
	case "show":
		return a.cmdActorShow(args[1:])
	case "create":
		return a.cmdActorCreate(args[1:])
	default:
		return fmt.Errorf("unknown actor command %q", args[0])
	}
}

func (a app) cmdActorShow(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: pactor actor show <alias>")
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	actor, err := actorWithCounts(rt, core.ActorID(strings.TrimPrefix(args[0], "@")))
	if err != nil {
		return err
	}
	if a.jsonOut {
		return writeJSON(a.out, actor)
	}
	printActor(a.out, actor)
	return nil
}

func (a app) cmdActorCreate(args []string) error {
	flags := flag.NewFlagSet("actor create", flag.ContinueOnError)
	flags.SetOutput(a.errOut)
	var internalID, legacyID, alias, name, role, filesystem string
	var routines stringList
	flags.StringVar(&alias, "alias", "", "unique actor alias")
	flags.StringVar(&legacyID, "id", "", "deprecated alias flag; use positional alias or --alias")
	flags.StringVar(&internalID, "internal-id", "", "optional generated-id override")
	flags.StringVar(&name, "name", "", "display name")
	flags.StringVar(&role, "role", "", "actor responsibility")
	flags.StringVar(&role, "responsibility", "", "actor responsibility")
	flags.StringVar(&filesystem, "fs", "", "actor filesystem root")
	flags.Var(&routines, "routine", "routine in form: name every 1m; repeatable")
	positionalAlias := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		positionalAlias = args[0]
		args = args[1:]
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	remaining := flags.Args()
	if legacyID != "" {
		if alias != "" || positionalAlias != "" || len(remaining) > 0 {
			return errors.New("actor alias provided twice")
		}
		alias = legacyID
	}
	if positionalAlias != "" {
		if alias != "" {
			return errors.New("actor alias provided twice")
		}
		alias = positionalAlias
	}
	if len(remaining) > 0 {
		if alias != "" {
			return fmt.Errorf("unexpected extra argument %q; quote multi-word --role and --routine values", remaining[0])
		}
		alias = remaining[0]
		remaining = remaining[1:]
	}
	if len(remaining) > 0 {
		return fmt.Errorf("unexpected extra argument %q; quote multi-word --role and --routine values", remaining[0])
	}
	if alias == "" || strings.TrimSpace(role) == "" {
		return errors.New("usage: pactor actor create <alias> --role ROLE [--name NAME] [--fs PATH] [--routine 'name every 1m']")
	}
	parsedRoutines, err := core.ParseRoutineInputs(strings.Join(routines, ", "))
	if err != nil {
		return err
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	actor, err := rt.CreateActor(context.Background(), core.CreateActorInput{
		ID:             core.ActorID(internalID),
		Alias:          alias,
		Name:           name,
		Responsibility: role,
		FilesystemRoot: filesystem,
		Routines:       parsedRoutines,
	})
	if err != nil {
		return err
	}
	if err := a.save(rt); err != nil {
		return err
	}
	if a.jsonOut {
		return writeJSON(a.out, actor)
	}
	fmt.Fprintf(a.out, "created actor:%s\nid: %s\nfilesystem: %s\n", actor.Alias, actor.ID, actor.FilesystemRoot)
	return nil
}

func (a app) cmdMailbox(args []string) error {
	flags := flag.NewFlagSet("mailbox", flag.ContinueOnError)
	flags.SetOutput(a.errOut)
	status := flags.String("status", "", "filter mailbox status")
	actorArg := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		actorArg = args[0]
		args = args[1:]
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if actorArg == "" && flags.NArg() > 0 {
		actorArg = flags.Arg(0)
	}
	if actorArg == "" || flags.NArg() > 1 {
		return errors.New("usage: pactor mailbox <actor> [--status queued]")
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	items, err := rt.ListMailbox(context.Background(), core.ActorID(strings.TrimPrefix(actorArg, "@")), core.MailboxFilter{Status: *status})
	if err != nil {
		return err
	}
	if a.jsonOut {
		return writeJSON(a.out, items)
	}
	tw := tabwriter.NewWriter(a.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSTATUS\tP\tTHREAD\tSUBJECT\tFROM")
	for _, item := range items {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\t%s\t%s\n", item.ID, item.Status, item.Priority, item.ThreadID, item.Subject, item.From)
	}
	return tw.Flush()
}

func (a app) cmdSend(args []string) error {
	flags := flag.NewFlagSet("send", flag.ContinueOnError)
	flags.SetOutput(a.errOut)
	to := flags.String("to", "", "target actor")
	subject := flags.String("subject", "", "thread subject")
	priority := flags.Int("priority", 60, "mailbox priority")
	if err := flags.Parse(args); err != nil {
		return err
	}
	bodyArgs := flags.Args()
	if *to == "" && len(bodyArgs) > 0 && isActorTarget(bodyArgs[0]) {
		*to = bodyArgs[0]
		bodyArgs = bodyArgs[1:]
	}
	body := strings.TrimSpace(strings.Join(bodyArgs, " "))
	if *to == "" || body == "" {
		return errors.New("usage: pactor send --to @actor [--subject SUBJECT] [--priority N] MESSAGE")
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	result, err := rt.PostMessage(context.Background(), core.PostMessageInput{
		From:     "user:todd",
		To:       []string{*to},
		Subject:  *subject,
		Body:     body,
		Kind:     "user_message",
		Priority: *priority,
	})
	if err != nil {
		return err
	}
	if err := a.save(rt); err != nil {
		return err
	}
	if a.jsonOut {
		return writeJSON(a.out, result)
	}
	fmt.Fprintf(a.out, "sent %s on %s; enqueued %d mailbox item(s)\n", result.MessageID, result.ThreadID, len(result.Enqueued))
	return nil
}

func (a app) cmdReply(args []string) error {
	flags := flag.NewFlagSet("reply", flag.ContinueOnError)
	flags.SetOutput(a.errOut)
	threadID := flags.String("thread", "", "thread id")
	to := flags.String("to", "", "target actor")
	if err := flags.Parse(args); err != nil {
		return err
	}
	bodyArgs := flags.Args()
	if *threadID == "" && len(bodyArgs) > 0 {
		*threadID = bodyArgs[0]
		bodyArgs = bodyArgs[1:]
	}
	body := strings.TrimSpace(strings.Join(bodyArgs, " "))
	if *threadID == "" || body == "" {
		return errors.New("usage: pactor reply --thread th_101 [--to @actor] MESSAGE")
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	if *to == "" {
		thread, err := rt.GetThread(context.Background(), core.ThreadID(*threadID))
		if err != nil {
			return err
		}
		*to = firstActorParticipant(thread.Participants)
	}
	if *to == "" {
		return errors.New("reply needs --to @actor because no actor participant was found")
	}
	result, err := rt.PostMessage(context.Background(), core.PostMessageInput{
		ThreadID: core.ThreadID(*threadID),
		From:     "user:todd",
		To:       []string{*to},
		Body:     body,
		Kind:     "user_message",
		Priority: 60,
	})
	if err != nil {
		return err
	}
	if err := a.save(rt); err != nil {
		return err
	}
	if a.jsonOut {
		return writeJSON(a.out, result)
	}
	fmt.Fprintf(a.out, "replied %s on %s; enqueued %d mailbox item(s)\n", result.MessageID, result.ThreadID, len(result.Enqueued))
	return nil
}

func (a app) cmdThreads(args []string) error {
	flags := flag.NewFlagSet("threads", flag.ContinueOnError)
	flags.SetOutput(a.errOut)
	actor := flags.String("actor", "", "filter actor id")
	status := flags.String("status", "", "filter thread status")
	if err := flags.Parse(args); err != nil {
		return err
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	threads, err := rt.ListThreads(context.Background(), core.ThreadFilter{
		ActorID: core.ActorID(strings.TrimPrefix(*actor, "@")),
		Status:  *status,
	})
	if err != nil {
		return err
	}
	if a.jsonOut {
		return writeJSON(a.out, threads)
	}
	tw := tabwriter.NewWriter(a.out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tSTATUS\tSUBJECT\tLAST")
	for _, thread := range threads {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", thread.ID, thread.Status, thread.Subject, summarizeCLI(thread.LastMessage, 72))
	}
	return tw.Flush()
}

func (a app) cmdThread(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: pactor thread show <id>")
	}
	switch args[0] {
	case "show":
		if len(args) != 2 {
			return errors.New("usage: pactor thread show <id>")
		}
		rt, err := a.load()
		if err != nil {
			return err
		}
		thread, err := rt.GetThread(context.Background(), core.ThreadID(args[1]))
		if err != nil {
			return err
		}
		if a.jsonOut {
			return writeJSON(a.out, thread)
		}
		printThread(a.out, thread)
		return nil
	default:
		return fmt.Errorf("unknown thread command %q", args[0])
	}
}

func (a app) cmdSession(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: pactor session <actor> <codex-session-id>")
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	sessionID := strings.TrimSpace(args[1])
	if err := rt.UpdateActor(context.Background(), core.ActorID(strings.TrimPrefix(args[0], "@")), core.ActorPatch{ActiveSessionID: &sessionID}); err != nil {
		return err
	}
	if err := a.save(rt); err != nil {
		return err
	}
	actor, err := rt.GetActor(context.Background(), core.ActorID(args[0]))
	if err != nil {
		return err
	}
	fmt.Fprintf(a.out, "actor:%s session: %s\n", actor.Alias, sessionID)
	return nil
}

func (a app) cmdRun(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(a.errOut)
	useCodex := flags.Bool("codex", false, "run through local Codex CLI")
	timeout := flags.Duration("timeout", 30*time.Minute, "run timeout")
	actorArg := ""
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		actorArg = args[0]
		args = args[1:]
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if actorArg == "" && flags.NArg() > 0 {
		actorArg = flags.Arg(0)
	}
	if actorArg == "" || flags.NArg() > 1 {
		return errors.New("usage: pactor run <actor> [--codex] [--timeout 30m]")
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	actorID := core.ActorID(strings.TrimPrefix(actorArg, "@"))
	item, err := rt.ClaimNext(ctx, actorID, time.Minute)
	if err != nil {
		return err
	}
	executor := core.Executor(core.PreviewCodexExecutor{})
	if *useCodex || os.Getenv("PACTOR_CODEX_CLI") == "1" {
		executor = codexcli.Executor{}
	}
	output, err := executor.Execute(ctx, item)
	if err != nil {
		_ = rt.Fail(context.Background(), item.ID, core.WorkError{Message: err.Error()})
		_ = a.save(rt)
		return err
	}
	if err := rt.Complete(context.Background(), item.ID, output); err != nil {
		return err
	}
	if err := a.save(rt); err != nil {
		return err
	}
	if a.jsonOut {
		return writeJSON(a.out, output)
	}
	fmt.Fprintf(a.out, "processed %s on %s\n\n%s\n", item.ID, item.ThreadID, output.Body)
	return nil
}

func (a app) cmdClose(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: pactor close <thread>")
	}
	rt, err := a.load()
	if err != nil {
		return err
	}
	if err := rt.UpdateThread(context.Background(), core.ThreadID(args[0]), core.ThreadPatch{Status: "resolved"}); err != nil {
		return err
	}
	if err := a.save(rt); err != nil {
		return err
	}
	fmt.Fprintf(a.out, "closed %s\n", args[0])
	return nil
}

func (a app) load() (*core.MemoryRuntime, error) {
	file, err := os.Open(a.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return core.NewMemoryRuntimeWithSandboxRoot(core.SeedDemoData(), a.sandboxRoot()), nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var snapshot core.MemorySnapshot
	if err := json.NewDecoder(file).Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode state %s: %w", a.statePath, err)
	}
	if strings.TrimSpace(snapshot.SandboxRoot) == "" {
		snapshot.SandboxRoot = a.sandboxRoot()
	}
	return core.NewMemoryRuntimeFromSnapshot(snapshot), nil
}

func (a app) save(rt *core.MemoryRuntime) error {
	if err := os.MkdirAll(filepath.Dir(a.statePath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rt.Snapshot(), "", "  ")
	if err != nil {
		return err
	}
	tmp := a.statePath + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, a.statePath)
}

func (a app) sandboxRoot() string {
	return filepath.Join(filepath.Dir(a.statePath), "sandboxes")
}

func defaultStatePath() string {
	if env := strings.TrimSpace(os.Getenv("PACTOR_STATE")); env != "" {
		return env
	}
	cwd, err := os.Getwd()
	if err != nil {
		return filepath.Join(".pactor", "state.json")
	}
	return filepath.Join(cwd, ".pactor", "state.json")
}

func (a app) printHelp() {
	fmt.Fprintln(a.out, `pactor - small CLI for actor mailbox experiments

Usage:
  pactor [--state .pactor/state.json] <command>

Commands:
  init [--force]                                      initialize local state
  actors                                              list actors
  actor show <alias>                                  show actor details
  actor create <alias> --role ROLE [--name NAME]      create actor
      [--fs PATH] [--routine "name every 1m"]
  send --to @actor [--subject SUBJECT] MESSAGE        send new thread message
  mailbox <actor> [--status queued]                   list actor mailbox
  threads [--actor actor] [--status open]             list threads
  thread show <id>                                    show thread messages
  reply --thread th_101 [--to @actor] MESSAGE         reply in a thread
  session <actor> <codex-session-id>                  bind Codex session
  run <actor> [--codex]                               process next mailbox item
  close <thread>                                      mark thread resolved
  state path                                          print state path

Default state: .pactor/state.json
Default actor filesystem root: .pactor/sandboxes/<actor-alias>`)
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func (s *stringList) String() string {
	return strings.Join(*s, ", ")
}

func actorWithCounts(rt *core.MemoryRuntime, actorID core.ActorID) (core.ActorView, error) {
	actor, err := rt.GetActor(context.Background(), actorID)
	if err != nil {
		return core.ActorView{}, err
	}
	actors, err := rt.ListActors(context.Background(), core.ActorFilter{})
	if err != nil {
		return core.ActorView{}, err
	}
	for _, summary := range actors {
		if summary.ID == actor.ID {
			actor.ActorSummary = summary
			break
		}
	}
	return actor, nil
}

func printActor(out io.Writer, actor core.ActorView) {
	fmt.Fprintf(out, "actor:%s\n", actor.Alias)
	fmt.Fprintf(out, "id: %s\n", actor.ID)
	fmt.Fprintf(out, "name: %s\n", actor.Name)
	fmt.Fprintf(out, "role: %s\n", actor.Role)
	fmt.Fprintf(out, "status: %s\n", actor.Status)
	fmt.Fprintf(out, "executor: %s\n", actor.Executor)
	fmt.Fprintf(out, "approval: %s\n", actor.ApprovalPolicy)
	fmt.Fprintf(out, "session: %s\n", emptyDash(actor.ActiveSessionID))
	fmt.Fprintf(out, "filesystem: %s\n", actor.FilesystemRoot)
	fmt.Fprintf(out, "mailbox: queued=%d running=%d\n", actor.MailboxQueued, actor.MailboxRunning)
	if len(actor.Routines) == 0 {
		fmt.Fprintln(out, "routines: none")
		return
	}
	fmt.Fprintln(out, "routines:")
	for _, routine := range actor.Routines {
		fmt.Fprintf(out, "  - %s every %s\n", routine.Name, routine.Every)
	}
}

func printThread(out io.Writer, thread core.ThreadView) {
	fmt.Fprintf(out, "thread:%s\n", thread.ID)
	fmt.Fprintf(out, "subject: %s\n", thread.Subject)
	fmt.Fprintf(out, "status: %s\n", thread.Status)
	fmt.Fprintf(out, "participants: %s\n", strings.Join(thread.Participants, ", "))
	for _, message := range thread.Messages {
		fmt.Fprintf(out, "\n[%s] %s -> %s (%s)\n%s\n",
			message.CreatedAt.Format(time.RFC3339),
			message.From,
			strings.Join(message.To, ","),
			message.Kind,
			message.Body,
		)
	}
}

func writeJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func firstActorParticipant(participants []string) string {
	for _, participant := range participants {
		if strings.HasPrefix(participant, "actor:") {
			return participant
		}
	}
	return ""
}

func isActorTarget(value string) bool {
	return strings.HasPrefix(value, "@") || strings.HasPrefix(value, "actor:")
}

func emptyDash(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func summarizeCLI(value string, limit int) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "\n", " "))
	if len(value) <= limit {
		return value
	}
	if limit < 4 {
		return value[:limit]
	}
	return value[:limit-3] + "..."
}
