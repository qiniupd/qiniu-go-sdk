# Gas SDK  

The SDK of Gas APIs.

## 接入流程

#### 1. 注册七牛账号

如果还没有七牛账号，可以在[这里](https://portal.qiniu.com/signup)注册

#### 2. 添加用户级别的配置信息

需要准备以下信息

```yaml
MaxBaseFee: BaseFee 阈值（高于这个数值一定不划算，避免极端的高 BaseFee），单位 `nanoFil`
MinerIDs: 当前账号所对应的 miner ID 列表
```

将帐号信息 + 以上配置信息同步给我们，我们在后台添加即可

#### 3. miner 通过 SDK 接入，完成动作（Action）的打点（标记）

Gas 提供基于 Golang 的 SDK，对应的 module 为 `github.com/qiniupd/qiniu-go-sdk/gas`，具体的配置及使用见下方 [Usage](#Usage)

关于动作（Action）的定义，详见下方 [Action](#Action)

这里通过引入 SDK，在每个 Action 的开始/完成时调用接口进行打点即可

#### 4. miner 通过 SDK 完成决策逻辑的接入

这个在完成打点接入且效果评估完成后再做，细节 TODO

## Usage

```golang
import (
	qgas "github.com/qiniupd/qiniu-go-sdk/gas"
)

func main() {

	// 对 SDK 的配置
	config := qgas.Config{
		MinerID:   "f23423", // 当前任务对应的 miner ID
		AccessKey: <Qiniu AccessKey>, // 用户在七牛的 AccessKey，可以在 https://portal.qiniu.com/user/key 页面查看
		SecretKey: <Qiniu SecretKey>, // 用户在七牛的 SecretKey https://portal.qiniu.com/user/key 页面查看
	}

	// 构造 SDK
	g := qgas.NewQGas(&config)

	// 当前 sealing 行为的标识（对当前 miner 唯一），可以使用 `SectorNumber` 作为 sealingID
	var sealingID string

	// 标记动作的开始
	g.StartAction(sealingID, qgas.ActionPreCommit) // g.StartAction(<SealingID>, <Action>)
	// 标记动作的结束
	g.EndAction(sealingID, qgas.ActionPreCommit) // g.EndAction(<SealingID>, <Action>)
	// 等待动作执行的合适时机（初期先不接入）
	g.Wait(sealingID, qgas.ActionSubmitProveCommit) // g.Wait(<SealingID>, <Action>)
	// 标记 sealing 过程的取消/中止（注意这个是 sealing 过程，而不是 sector 生命周期的中止）
	g.CancelSealing(sealingID) // g.CancelSealing(<SealingID>)
}
```

## Action

Action 是 sector sealing 过程中的动作，每个 Action 都会有开始和完成，通过对每个 sealing 过程 Action 的开始 & 完成时间点的记录，我们可以了解宏观的生产、堆积情况，以便做出决策；目前我们关心的 Action 有以下四个：

### ActionPreCommit

对应 Pre Commit 的过程，包括 preCommit phase 1 & preCommit phase 2

### ActionSubmitPreCommit

对应 `PreCommitSector` 消息提交上链的过程，在广播消息前标记动作的开始，在确认消息成功上链后标记动作的完成

### ActionCommit

对应 Commit 的过程，包括 commit phase 1 & commit phase 2，标记该动作开始时 `WaitSeed` 的过程应当已完成，标记该动作结束时意味着可以马上开始做 `ProveCommitSector` 消息的上链

### ActionSubmitProveCommit

对应 `ProveCommitSector` 消息提交上链的过程，在广播消息前标记动作的开始，在确认消息成功上链后标记动作的完成
