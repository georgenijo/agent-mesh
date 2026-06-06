package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

func runAsk(args []string, stdout, stderr io.Writer) int {
	var role, to, ctx, ttl string
	var wait bool
	vs, code, err := setupVerb("ask", args, stderr, func(fs *flag.FlagSet) {
		fs.StringVar(&role, "role", "", "target role")
		fs.StringVar(&to, "to", "", "target agent id")
		fs.StringVar(&ctx, "ctx", "", "optional context")
		fs.StringVar(&ttl, "ttl", "", "ticket TTL duration")
		fs.BoolVar(&wait, "wait", false, "wait for answer (explicit only)")
	})
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return code
	}
	if len(vs.positional) != 1 || vs.positional[0] == "" {
		fmt.Fprintln(stderr, `usage: mesh ask (--role R | --to ID) "<question>"`)
		return ExitUsage
	}
	resp, code, err := doVerb(vs.socketPath, meshapi.VerbAsk,
		meshapi.AskArgs{Role: role, To: to, Question: vs.positional[0], Context: ctx, TTL: ttl, Wait: wait})
	if err != nil {
		return emit(stdout, stderr, vs.jsonOut, resp, code, err, nil)
	}
	var res meshapi.AskVerbResult
	if err := json.Unmarshal(resp.Data, &res); err != nil {
		fmt.Fprintln(stderr, "mesh: bad ask response:", err)
		return ExitError
	}
	if vs.jsonOut {
		if wait {
			return waitForAnswer(vs.socketPath, res.Ticket, stdout, stderr, true)
		}
		fmt.Fprintln(stdout, string(resp.Data))
		return ExitOK
	}
	if wait {
		fmt.Fprintf(stdout, "ticket %s\n", res.Ticket)
		return waitForAnswer(vs.socketPath, res.Ticket, stdout, stderr, false)
	}
	fmt.Fprintf(stdout, "ticket %s\n", res.Ticket)
	return ExitOK
}

func waitForAnswer(socketPath, ticketID string, stdout, stderr io.Writer, jsonOut bool) int {
	deadline := time.Now().Add(defaultWaitLimit)
	for {
		resp, code, err := doVerb(socketPath, meshapi.VerbPoll, meshapi.PollArgs{Ticket: ticketID})
		if err != nil {
			return emit(stdout, stderr, jsonOut, resp, code, err, nil)
		}
		var res meshapi.PollResult
		if err := json.Unmarshal(resp.Data, &res); err != nil {
			fmt.Fprintln(stderr, "mesh: bad poll response:", err)
			return ExitError
		}
		switch res.Result {
		case envelope.AskPending:
			if time.Now().After(deadline) {
				if jsonOut {
					fmt.Fprintln(stdout, string(resp.Data))
				} else {
					fmt.Fprintln(stderr, "mesh: no answer yet")
				}
				return ExitNoAnswer
			}
			time.Sleep(500 * time.Millisecond)
		case envelope.AskNoSuchTicket:
			if jsonOut {
				fmt.Fprintln(stdout, string(resp.Data))
			} else {
				fmt.Fprintln(stderr, "mesh: no such ticket")
			}
			return ExitNoTicket
		default:
			if jsonOut {
				fmt.Fprintln(stdout, string(resp.Data))
			} else if res.Answer != "" {
				fmt.Fprintln(stdout, res.Answer)
			}
			return ExitOK
		}
	}
}

const defaultWaitLimit = 30 * time.Minute

func runPoll(args []string, stdout, stderr io.Writer) int {
	vs, code, err := setupVerb("poll", args, stderr, nil)
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return code
	}
	if len(vs.positional) != 1 || vs.positional[0] == "" {
		fmt.Fprintln(stderr, `usage: mesh poll <ticket>`)
		return ExitUsage
	}
	resp, code, err := doVerb(vs.socketPath, meshapi.VerbPoll, meshapi.PollArgs{Ticket: vs.positional[0]})
	if err != nil {
		return emit(stdout, stderr, vs.jsonOut, resp, code, err, nil)
	}
	var res meshapi.PollResult
	if err := json.Unmarshal(resp.Data, &res); err != nil {
		fmt.Fprintln(stderr, "mesh: bad poll response:", err)
		return ExitError
	}
	exit := ExitOK
	if res.Result == envelope.AskPending {
		exit = ExitNoAnswer
	}
	if res.Result == envelope.AskNoSuchTicket {
		exit = ExitNoTicket
	}
	if vs.jsonOut {
		fmt.Fprintln(stdout, string(resp.Data))
		return exit
	}
	switch res.Result {
	case envelope.AskAnswered:
		fmt.Fprintln(stdout, res.Answer)
	case envelope.AskPending:
		fmt.Fprintln(stderr, "mesh: no answer yet")
	case envelope.AskNoSuchTicket:
		fmt.Fprintln(stderr, "mesh: no such ticket")
	default:
		fmt.Fprintf(stderr, "mesh: ticket %s\n", res.Result)
	}
	return exit
}

func runInbox(args []string, stdout, stderr io.Writer) int {
	var limit int
	var watch bool
	vs, code, err := setupVerb("inbox", args, stderr, func(fs *flag.FlagSet) {
		fs.IntVar(&limit, "limit", 0, "max items")
		fs.BoolVar(&watch, "watch", false, "wait until at least one item is available")
	})
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return code
	}
	if len(vs.positional) != 0 {
		fmt.Fprintln(stderr, `usage: mesh inbox [--limit N] [--watch]`)
		return ExitUsage
	}
	for {
		resp, code, err := doVerb(vs.socketPath, meshapi.VerbInbox, meshapi.InboxArgs{Limit: limit, Watch: watch})
		if err != nil {
			return emit(stdout, stderr, vs.jsonOut, resp, code, err, nil)
		}
		var res meshapi.InboxResult
		if err := json.Unmarshal(resp.Data, &res); err != nil {
			fmt.Fprintln(stderr, "mesh: bad inbox response:", err)
			return ExitError
		}
		if !watch || len(res.Items) > 0 {
			if vs.jsonOut {
				fmt.Fprintln(stdout, string(resp.Data))
				return ExitOK
			}
			printInbox(stdout, res)
			return ExitOK
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func printInbox(w io.Writer, res meshapi.InboxResult) {
	if len(res.Items) == 0 {
		fmt.Fprintln(w, "inbox empty")
		return
	}
	tw := tabwriter.NewWriter(w, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TICKET\tFROM\tQUESTION")
	for _, item := range res.Items {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", item.Ticket, item.From, item.Question)
	}
	tw.Flush() //nolint:errcheck
}

func runAnswer(args []string, stdout, stderr io.Writer) int {
	vs, code, err := setupVerb("answer", args, stderr, nil)
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return code
	}
	if len(vs.positional) != 2 || vs.positional[0] == "" || vs.positional[1] == "" {
		fmt.Fprintln(stderr, `usage: mesh answer <ticket> "<answer>"`)
		return ExitUsage
	}
	resp, code, err := doVerb(vs.socketPath, meshapi.VerbAnswer,
		meshapi.AnswerArgs{Ticket: vs.positional[0], Answer: vs.positional[1]})
	if err != nil {
		return emit(stdout, stderr, vs.jsonOut, resp, code, err, nil)
	}
	var res meshapi.AnswerVerbResult
	if err := json.Unmarshal(resp.Data, &res); err != nil {
		fmt.Fprintln(stderr, "mesh: bad answer response:", err)
		return ExitError
	}
	exit := ExitOK
	if res.Result == envelope.AskNoSuchTicket {
		exit = ExitNoTicket
	}
	if vs.jsonOut {
		fmt.Fprintln(stdout, string(resp.Data))
		return exit
	}
	if res.Result == envelope.AskNoSuchTicket {
		fmt.Fprintln(stderr, "mesh: no such ticket")
		return exit
	}
	fmt.Fprintf(stdout, "answered %s\n", res.Ticket)
	return exit
}
