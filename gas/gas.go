package gas

import (
	"errors"
	"time"

	clt "github.com/qiniupd/qiniu-go-sdk/gas/client"
	cfg "github.com/qiniupd/qiniu-go-sdk/gas/config"
	lgr "github.com/qiniupd/qiniu-go-sdk/gas/logger"
)

const (
	// ActionPreSealSector 对应 Pre-seal 的过程
	ActionPreSealSector = "PreSealSector"
	// ActionPreCommitSector 对应 PreCommitSector 消息上链的过程
	ActionPreCommitSector = "PreCommitSector"
	// ActionCommitting 对应 Committing 的过程
	ActionCommitting = "Committing"
	// ActionProveCommitSector 对应 ProveCommitSector 消息上链的过程
	ActionProveCommitSector = "ProveCommitSector"
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

type Config = cfg.Config

func NewQGas(config *Config) *QGas {
	client := clt.NewClient(config)
	q := &QGas{
		config: config,
		client: client,
	}
	q.SetLogger(&lgr.DefaultLogger{})
	return q
}

// SetLogger 设置 logger
func (q *QGas) SetLogger(logger lgr.Logger) {
	q.logger = logger
	q.client.SetLogger(logger)
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

// Wait 会阻塞当前工作，直到系统认为当前时间适合执行目标 action
func (q *QGas) Wait(sealingID string, action string) error {
	for ok := false; !ok; {
		checked, err := q.client.CheckAction(sealingID, action, nil)
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

// GetScheduledTime 获取到在指定时间（t）后适合执行目标 action 的时间点
// 只对历史数据有效，即，执行时间点早于当前时间才能被获取到
func (q *QGas) GetScheduledTime(sealingID string, action string, t int64) (int64, error) {
	now := time.Now().Unix()
	for checkAt := t; checkAt < now; {
		checked, err := q.client.CheckAction(sealingID, action, &checkAt)
		if err != nil {
			return 0, err
		}
		if checked.Ok {
			return checkAt, nil
		}
		checkAt = checkAt + int64(checked.Wait)
	}
	return 0, errors.New("scheduled time not found")
}

// UserConfig 是用户级别的配置信息
type UserConfig = clt.UserConfig

// GetUserConfig 用来获取用户级别的配置信息
func (q *QGas) GetUserConfig() (*clt.UserConfig, error) {
	return q.client.GetUserConfig()
}

// SetUserConfig 用来设置用户级别的配置信息
func (q *QGas) SetUserConfig(userConfig *clt.UserConfig) error {
	return q.client.SetUserConfig(userConfig)
}
