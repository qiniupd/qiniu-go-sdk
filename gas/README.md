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

Gas 提供基于 Golang 的 SDK，对应的 module 为 `github.com/qiniupd/qiniu-go-sdk/gas`，这里通过引入 SDK，在每个 Action 的开始/完成时调用接口进行打点即可；具体的配置及使用如下

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
	// 标记 sealing 过程的取消/中止（注意这里不是指 sector 生命周期的中止）
	g.CancelSealing(sealingID) // g.CancelSealing(<SealingID>)
}
```

关于动作（Action）的定义，详见下方 [Action](#Action)

标记 sealing 过程的取消（`CancelSealing`）的合适的时机是决定放弃某个未完成的 sealing 过程、不再继续时；以 Lotus 为例，适合执行 `CancelSealing` 的时机一般是以下两处：

1. storage-sealing 模块中对 event 进行 plan 遇到错误的时候：

	https://github.com/filecoin-project/lotus/blob/50b4ea30836618d199e24024f4f591de3dfbb516/extern/storage-sealing/fsm.go#L28

	```golang
	if err != nil {
		log.Errorf("unhandled sector error (%d): %+v", si.SectorNumber, err)
		// 一般来说这里发生错误不会再有重试的机制了，故标记下取消
		g.CancelSealing(sealingID)
		return nil
	}
	```

2. 对未成功封装的 sector 执行清除动作的时候

#### 4. miner 通过 SDK 完成决策逻辑的接入

决策逻辑预期在完成打点接入且基于打点数据完成效果评估完成后接入

通过在执行具体的 Action 前调用 SDK 提供的 `Wait` 方法，可以接入七牛提供的决策逻辑；`Wait` 会阻塞当前的 goroutine，直到当前时机被判定为适合执行目标动作，具体代码示例如下：

```golang
func main() {
	// 完成 Commit 的工作

	// 等待 Action 执行的合适时机，目前我们只支持对于 Action SubmitProveCommit 的决策
	// 这里的 g 即上边通过 qgas.NewQGas(&config) 构造出来的实例
	g.Wait(sealingID, qgas.ActionSubmitProveCommit) // g.Wait(<SealingID>, <Action>)

	// 将 ProveCommit 消息上链
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
