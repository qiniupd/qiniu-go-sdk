# 可靠批量复制/删除脚本

实现一个批量复制/删除脚本，能处理上百万个文件的复制，删除，在执行期间脚本如果被正常或非正常关闭，重启后依然可以继续执行，不会从头开始。Qshell v2 目前没有实现这方面的功能，一旦在执行期间中断，无法恢复，且无法判断究竟哪些已经执行完毕，哪些未能执行完毕。该脚本可以作为 Qshell v3 任务管理器模块的原型。

### 编译方式

在 https://github.com/qiniupd/qiniu-go-sdk 中 `syncdata/cmd/batch` 目录下，`go build .` 即可编译得到 batch 工具的可执行文件

### 使用方式

```
./batch CMD [INPUTFILE] [OPTIONS]
```

- CMD 支持三种命令 `stat` / `copy` / `delete` / `jobs` / `continue`
- INPUTFILE 要求指定格式的 CSV 文件，作为参数输入文件。
  - 其中 `stat` 命令接受单列 CSV 文件，表示需要列举的文件列表。
  - `copy` 命令接受双列 CSV 文件，左列表示复制源文件路径，右列表示复制目标文件路径。
  - `delete` 命令接受单列 CSV 文件，表示需要删除的文件列表。
  - `jobs` 命令可以选择接受 INPUTFILE 参数，支持指定 JOBID。如果指定了 JOBID，则展示单个任务及其子任务的详情，否则，展示任务列表，不展示子任务详情。
  - `continue` 命令在 INPUTFILE 参数的位置指定 JOBID。
- OPTIONS 表示命令的选项
  - --config-file 表示 IPFS Go SDK 的配置文件路径，默认为当前目录的 cfg.toml 文件
  - --concurrency 表示最大并发请求数量，默认为 1
  - --db-file 表示进度数据文件路径，默认为当前目录的 `batch.db` 文件
  - --log-file 表示日志文件路径，默认为当前目录的 `batch.log` 文件
  - --log-level 表示日志级别，默认为 `warn`，可选值为 `debug` / `info` / `warn` / `error`
  - --batch-size 表示批处理规模，默认为 100，不可大于 1000，该选项对 `jobs` 和 `continue` 命令不生效
  - --only-failed 表示只展示失败的子任务的详情，只对 `jobs` 命令生效
  - --bucket-dest 表示复制文件到指定存储空间，默认与配置文件内的存储空间相同
  - --migrate-db 表示迁移进度数据文件，默认不会迁移，注意：只有新版本导致数据库表结构变更的时候，才会需要使用该命令。迁移数据文件为危险操作，建议在开发人员的指导下进行
