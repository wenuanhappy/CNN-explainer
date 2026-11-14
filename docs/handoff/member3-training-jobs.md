# 成员3 训练任务与资源控制：Agent 交接文档

> 维护规则：每个 agent 开始工作前先完整阅读本文档；每完成一个任务或一次提交后，立即更新本文档并与代码放在同一个提交中；结束会话前必须更新「下一步」和「会话日志」。

## 基本信息

- 分支：`feature/member3-training-job-control`（基于 `origin/main` @ `32a7d80`）
- 最近更新：2026-11-14 13:37（初始化）
- 当前状态：未开始（分支与交接文档已建立，尚无代码改动）
- PR：未创建
- 任务依据：《DeepVision Studio 五人小组任务分工与验收安排》第 6 节（成员3），只做 P0，不做 P1

## 任务进度

| 编号 | 内容 | 状态 | 相关提交 | 备注 |
| --- | --- | --- | --- | --- |
| T3-01 | 持久化任务（实体 + Repository） | 未开始 | | |
| T3-02 | 状态机与转换规则、暂停资源策略 | 未开始 | | |
| T3-03 | 并发 / 用户活动数 / 队列长度 / 超时配置 | 未开始 | | 本地默认并发 1 |
| T3-04 | 幂等启动 | 未开始 | | |
| T3-05 | 控制受理与 worker 执行确认分离 | 未开始 | | 终态 resume 返回 409 |
| T3-06 | 启动失败、非零退出、停止超时、残留进程、槽位释放 | 未开始 | | |
| T3-07 | 重启后未知活动任务标记 interrupted | 未开始 | | 不做精确续训 |
| T3-08 | 实际设备上报、友好的 OOM 提示、CPU 路径 | 未开始 | | |
| T3-09 | 事件 `jobId/sequence/timestamp/schemaVersion`，以及状态和历史查询 | 未开始 | | |

## 现状核查（基于 main @ 32a7d80，供参考，开始前请再确认）

- `backend/spring/.../training/TrainingJobService.java`（约 1045 行）
  - 任务只保存在内存里的 `ConcurrentHashMap<String, TrainingJob> jobs` 中，服务重启后丢失。
  - `start()` 直接用 `ProcessBuilder` 为每个任务启动一个 `training_worker.py`，没有并发上限和队列，也没有幂等。
  - `pause/resume/stop` 只调用 `writeControl(job, "...")` 写 controlFile，然后立即修改并返回状态，没有等 worker 确认。
  - `stop/reset` 直接调用 `destroyProcess()`；`@PreDestroy` 销毁全部进程。
- `TrainingController.java`：`/api/training/start`，以及 `/{jobId}/status|pause|resume|stop|reset|save|weights/histogram`。
- `TrainingStreamHandler.java` / `TrainingWebSocketConfig.java`：WebSocket `/api/training/stream`。
- `backend/python-training/training_worker.py`（约 1398 行）
  - 用 `read_control` / `wait_if_paused` 轮询 controlFile（JSON）。
  - stdout 输出 JSON 事件：`metric / backprop / test_result / control / error`。
  - 设备：`torch.device("cuda" if torch.cuda.is_available() else "cpu")`，未上报给后端。
- 测试：没有 `backend/spring/src/test` 目录；`pom.xml` 中未见 `spring-boot-starter-test`。
- 技术栈：Spring Boot 3.3.5、Java 17、Maven、H2、JJWT 0.12.6。

## 协作边界

- 成员2：网络校验、模型构建、协议解析（包括 `training_worker.py` 的模型与协议部分、`TrainingDtos.java` 的网络字段）。本分支只在创建进程前预留校验接口。
- 成员4：前端 `frontend/`。本分支不改前端；响应和事件只新增字段，不删除也不重命名已有字段。
- 成员5：测试基础设施与 CI。本分支加入的测试依赖或测试配置要在下文登记。

## 设计决策（含原因，按时间追加）

- （暂无）

## 接口与协议变更（给成员2/4/5）

- 新增/变更的 REST 接口：（暂无）
- 请求头/请求体字段（如幂等键）：（暂无）
- 状态枚举：（暂无）
- 事件字段：（暂无）
- 错误码与错误结构（`code/message/details/requestId`，由成员2牵头，此处只记录临时约定）：（暂无）
- 配置项及默认值：（暂无）

## 改动文件清单

- `docs/handoff/member3-training-jobs.md`（新增，本文档）

## 运行与测试方法

- Java：`cd backend/spring; mvn -q test`
- Python：`cd backend/python-training; python -m pytest`（测试文件位置待定）
- 实际执行结果记录在下方「验收记录」中。

## 验收记录

| 验收编号 | 环境与输入 | 操作或命令 | 预期 | 实际 | 证据位置 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| AC-03.1 | | 同一个幂等键提交两次 | 只有 1 个任务，最多 1 个进程 | | | 未执行 |
| AC-03.2 | 测试配置：并发 2、队列 3 | 同时发出 6 个请求 | 最多 2 个执行、3 个排队，其余收到容量错误 | | | 未执行 |
| AC-03.3 | | 暂停 / 继续 / 停止；对已完成任务调用 resume | 经确认后才变更状态；resume 返回 409 | | | 未执行 |
| AC-03.4 | | 强制杀掉 worker；模拟服务重启 | 状态为 failed / interrupted，并有原因 | | | 未执行 |
| AC-03.5 | | 任务结束或停止 | 释放槽位，没有遗留的未知受管进程 | | | 未执行 |

结论只能填：通过 / 失败 / 阻塞 / 未执行。

## 已知问题 / 风险 / 待确认事项

- 错误结构和 `schemaVersion` 规则需要与成员2确认。
- 新增的状态值（包括 pausing/stopping 等过渡状态）需要与成员4确认前端如何展示。
- pom.xml 加入测试依赖的方式需要与成员5确认。

## 下一步（按优先级，下一个 agent 从这里开始）

1. 阅读 `CLAUDE.md`、`docs/development-document.md`、`TrainingJobService.java`、`TrainingController.java`、`training_worker.py` 的控制部分，核对上面的「现状核查」。
2. 设计任务实体和状态转换表，写进「设计决策」。
3. 按提示词第 6 节的顺序小步实现并提交。

## 会话日志

- 2026-11-14 13:37：初始化。从 origin/main 建立分支，创建本交接文档，还没有改代码。
