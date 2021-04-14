package cmds

import (
	"context"
	"fmt"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/cmd/batch/db"
)

const ContinueCmdName = "continue"

type ContinueCmd struct {
	BaseCmd
	JobId uint64
}

func (cmd *ContinueCmd) GetJob(ctx context.Context) (Job, error) {
	var (
		dbRecord db.Job
		err      error
	)
	if err = cmd.Database.First(&dbRecord, cmd.JobId).Error; err != nil {
		return nil, err
	}
	switch dbRecord.Cmd {
	case StatCmdName:
		statJob := new(StatJob)
		statJob.fromDBRecord(&dbRecord)
		statJob.Cmd = &StatCmd{BaseCmd: cmd.BaseCmd, IsContinue: true, Job: statJob}
		return statJob, nil
	case CopyCmdName:
		copyJob := new(CopyJob)
		copyJob.fromDBRecord(&dbRecord)
		copyJob.Cmd = &CopyCmd{BaseCmd: cmd.BaseCmd, IsContinue: true, Job: copyJob}
		return copyJob, nil
	case DeleteCmdName:
		deleteJob := new(DeleteJob)
		deleteJob.fromDBRecord(&dbRecord)
		deleteJob.Cmd = &DeleteCmd{BaseCmd: cmd.BaseCmd, IsContinue: true, Job: deleteJob}
		return deleteJob, nil
	default:
		return nil, fmt.Errorf("unrecognized command of job: %s", dbRecord.Cmd)
	}
}
