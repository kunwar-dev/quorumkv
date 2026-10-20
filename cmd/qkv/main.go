// Command qkv is the quorumkv command-line client.
//
//	qkv put greeting hello
//	qkv get greeting
//	qkv del greeting
//	qkv status
//	qkv bench -n 20000 -c 32 -size 128 -writes 0.5
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kunwar-dev/quorumkv/client"
	"github.com/kunwar-dev/quorumkv/server"
)

const usage = `qkv - quorumkv client

Usage:
  qkv [flags] get <key>
  qkv [flags] put <key> <value>     (value "-" reads stdin)
  qkv [flags] del <key>
  qkv [flags] status
  qkv [flags] bench [bench flags]

Flags:
`

func main() {
	endpoints := flag.String("endpoints", envOr("QKV_ENDPOINTS", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"), "comma-separated host:port list (env QKV_ENDPOINTS)")
	timeout := flag.Duration("timeout", 10*time.Second, "per-command timeout")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	eps := strings.Split(*endpoints, ",")
	if err := run(eps, *timeout, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "qkv:", err)
		os.Exit(1)
	}
}

func run(endpoints []string, timeout time.Duration, args []string) error {
	cmd, args := args[0], args[1:]
	if cmd == "bench" {
		return runBench(endpoints, args)
	}
	if cmd == "status" {
		return status(endpoints)
	}

	c, err := client.New(endpoints)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch {
	case cmd == "get" && len(args) == 1:
		v, err := c.Get(ctx, args[0])
		if err != nil {
			return err
		}
		os.Stdout.Write(v)
		if len(v) > 0 && v[len(v)-1] != '\n' {
			fmt.Println()
		}
		return nil
	case cmd == "put" && len(args) == 2:
		value := []byte(args[1])
		if args[1] == "-" {
			if value, err = io.ReadAll(os.Stdin); err != nil {
				return err
			}
		}
		return c.Put(ctx, args[0], value)
	case (cmd == "del" || cmd == "delete") && len(args) == 1:
		return c.Delete(ctx, args[0])
	default:
		flag.Usage()
		return errors.New("invalid command")
	}
}

func status(endpoints []string) error {
	hc := &http.Client{Timeout: 2 * time.Second}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ENDPOINT\tID\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tSNAPSHOT\tKEYS")
	for _, ep := range endpoints {
		resp, err := hc.Get("http://" + ep + "/v1/status")
		if err != nil {
			fmt.Fprintf(tw, "%s\t-\tunreachable\t\t\t\t\t\t\n", ep)
			continue
		}
		var st server.StatusResponse
		err = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if err != nil {
			fmt.Fprintf(tw, "%s\t-\tbad response\t\t\t\t\t\t\n", ep)
			continue
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%d\t%d\t%d\t%d\n",
			ep, st.ID, st.Role, st.Term, st.Leader, st.CommitIndex, st.AppliedIndex, st.SnapshotIndex, st.Keys)
	}
	return tw.Flush()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
