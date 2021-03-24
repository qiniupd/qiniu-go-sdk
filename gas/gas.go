package gas

import (
	"time"

	clt "github.com/qiniupd/qiniu-go-sdk/gas/client"
	cfg "github.com/qiniupd/qiniu-go-sdk/gas/config"
	lgr "github.com/qiniupd/qiniu-go-sdk/gas/logger"
)

const (
	// ActionPreCommit 对应 Pre Commit 的过程
	ActionPreCommit = "PreCommit"
	// ActionSubmitPreCommit 对应 PreCommitSector 消息上链的过程
	ActionSubmitPreCommit = "SubmitPreCommit"
	// ActionCommit 对应 Commit 的过程
	ActionCommit = "Commit"
	// ActionSubmitProveCommit 对应 ProveCommitSector 消息上链的过程
	ActionSubmitProveCommit = "SubmitProveCommit"
)

const (
	actionStatusStart = "Start"
	actionStatusEnd   = "End"
)

type QGas struct {
	config *cfg.Config
	client *clt.Client
	logger lgr.Logger
}

// Config 是对于 SDK 行为的配置
type Config = cfg.Config

// NewQGas 用于构造 QGas 对象
func NewQGas(config *Config) *QGas {
	cfg := *config
	if cfg.Logger == nil {
		cfg.Logger = &lgr.DefaultLogger{}
	}
	client := clt.NewClient(&cfg)
	q := &QGas{
		config: &cfg,
		client: client,
		logger: cfg.Logger,
	}
	return q
}

// StartAction 标记动作的开始
func (q *QGas) StartAction(sealingID, action string) error {
	return q.client.UpdateAction(sealingID, action, actionStatusStart)
}

// EndAction 标记动作的结束
func (q *QGas) EndAction(sealingID, action string) error {
	return q.client.UpdateAction(sealingID, action, actionStatusEnd)
}

// SealingData 是 sealing 条目的内容
type SealingData = clt.SealingData

// GetSealing 获取当前 sector sealing 条目信息
func (q *QGas) GetSealing(sealingID string) (*SealingData, error) {
	return q.client.GetSealing(sealingID)
}

// CancelSealing 标记当前 sector sealing 行为取消
func (q *QGas) CancelSealing(sealingID string) error {
	return q.client.CancelSealing(sealingID)
}

type CheckActionData = clt.CheckActionData

func (q *QGas) CheckAction(sealingID string, action string, t *int64) (*CheckActionData, error) {
	return q.client.CheckAction(sealingID, action, t)
}

// Wait 会阻塞当前工作，直到系统认为当前时间适合执行目标 action
func (q *QGas) Wait(sealingID string, action string) error {
	for ok := false; !ok; {
		checked, err := q.CheckAction(sealingID, action, nil)
		if err != nil {
			// TODO, 重试？
			return err
		}
		if checked.Ok {
			ok = true
		} else {
			time.Sleep(time.Duration(checked.Wait) * time.Second)
		}
	}
	return nil
}

func (q *QGas) Request(method, path string, reqData interface{}, respData interface{}) (err error) {
	return q.client.Request(method, path, reqData, respData)
}
