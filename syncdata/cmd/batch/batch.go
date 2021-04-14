package main

import (
	"context"
	"fmt"
	"os"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/flags"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/worker_pool"
)

func main() {
	cmd, parser, err := flags.ParseFlags(context.Background(), os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Parse flags error: %s\n", err)
		if _, ok := err.(flags.CmdError); ok {
			parser.WriteHelp(os.Stderr)
		}
		os.Exit(1)
	}
	job, err := cmd.GetJob(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get job error: %s\n", err)
		os.Exit(1)
	}
	microJobs, err := job.GetMicroJobs(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get micro jobs error: %s\n", err)
		os.Exit(1)
	}
	worker_pool.FinishAllJobs(context.Background(), cmd.GetConcurrency(), microJobs, os.Stdout, cmd.GetLogger())
}
