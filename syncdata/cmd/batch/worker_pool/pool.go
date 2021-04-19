package worker_pool

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

	qiniu_kodo_client_sdk "github.com/qiniupd/qiniu-go-sdk/api.v8/kodocli"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/cmds"
	"golang.org/x/sync/semaphore"
)

func FinishAllJobs(ctx context.Context, concurrency uint, executables []cmds.MicroJob, output io.Writer, logger qiniu_kodo_client_sdk.Ilog) {
	sem := semaphore.NewWeighted(int64(concurrency))
	uncompleted := int64(len(executables))
	logReadersChan := make(chan io.Reader, len(executables))
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for logReader := range logReadersChan {
			if _, err := io.Copy(output, logReader); err != nil {
				logger.Error("Failed to copy logger: %v", err)
			}
		}
	}()

	for _, executable := range executables {
		wg.Add(1)
		if err := sem.Acquire(ctx, 1); err != nil {
			logger.Error("Failed to acquire semaphore: %v", err)
			return
		}
		go func(executable cmds.MicroJob) {
			defer func() {
				if atomic.AddInt64(&uncompleted, -1) == 0 {
					close(logReadersChan)
				}
				sem.Release(1)
				wg.Done()
			}()
			logReadersChan <- executable.Execute(ctx)
		}(executable)
	}

	wg.Wait()
}
