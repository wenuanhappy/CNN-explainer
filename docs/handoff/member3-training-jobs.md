# 成员3 训练任务与资源控制：Agent 交接文档

> 维护规则：每个 agent 开始工作前先完整阅读本文档；每完成一个任务或一次提交后，立即更新本文档并与代码放在同一个提交中；结束会话前必须更新「下一步」和「会话日志」。

## 基本信息

- 分支：`feature/member3-training-job-control`（基于 `origin/main` @ `32a7d80`）
- 最近更新：2026-11-14 17:00 by member3-followup-agent
- 当前状态：评审修复全部完成，待手动推送更新远程分支与 PR（已完成 T3-01 ~ T3-09 及评审项 B1 ~ B3、S1 ~ S4、全部 5 项次要项；28 项 Java 测试 + 7 项 Python 测试全部通过；按用户指示最后一步由用户手动执行 `git push`）
- PR：**必须与成员4的前端适配一起合并，或在成员4的前端适配之后合并**（见下方「合并前置条件：前端适配清单（交给成员4）」与「PR 描述补充」）
- 任务依据：《DeepVision Studio 五人小组任务分工与验收安排》第 6 节（成员3），只做 P0，不做 P1

## 任务进度

| 编号 | 内容 | 状态 | 相关提交 | 备注 |
| --- | --- | --- | --- | --- |
| T3-01 | 持久化任务（实体 + Repository） | 完成 | `3e154ba` | `TrainingJob` + `TrainingJobRepository` + `application-test.yml` |
| T3-02 | 状态机与转换规则、暂停资源策略 | 完成 | `4249500` | 集中转换表 `TrainingJobStateMachine` + `pendingControl` + 409 错误契约 + `NetworkValidator` 预留 |
| T3-03 | 并发 / 用户活动数 / 队列长度 / 超时配置 | 完成 | `4760840` | `TrainingProperties` + `TrainingProcessLauncher` + 并发槽位与有界 FIFO 队列，本地默认并发 1 |
| T3-04 | 幂等启动 | 完成 | `082b725` | 请求头 `Idempotency-Key` + `configDigest` 一致性校验 + 409 `IDEMPOTENCY_KEY_CONFLICT` |
| T3-05 | 控制受理与 worker 执行确认分离 | 完成 | `0a4d9bf` | `pause/resume/stop` 返回已受理并置 `pendingControl`，经 worker `control` 事件确认后切最终态；`stopTimeout` 超时强杀；终态 `resume` 返 409 |
| T3-06 | 启动失败、非零退出、停止超时、残留进程、槽位释放 | 完成 | `24d0730` | 覆盖启动异常、非零退出、`jobTimeout`、`stopTimeout`、子进程树清理与全路径槽位回收 |
| T3-07 | 重启后未知活动任务标记 interrupted | 完成 | `24d0730` | 启动时扫描无受管存活进程的 `queued/running/paused` 任务置为 `interrupted`（不做续训） |
| T3-08 | 实际设备上报、友好的 OOM 提示、CPU 路径 | 完成 | `83339a2` | worker 上报 `device`（`cuda`/`cpu`）、补全 `paused/running/stopped` 确认、CUDA OOM 退出码 `2` 与友好建议、CPU 训练单测 |
| T3-09 | 事件 `jobId/sequence/timestamp/schemaVersion`，以及状态和历史查询 | 完成 | `0b311c4` | 统一对外事件信封 + `TrainingJobEvent` 有界历史表 + `GET /api/training/{jobId}/events` |

### 评审修复进度

| 编号 | 内容 | 状态 | 相关提交 | 备注 |
| --- | --- | --- | --- | --- |
| B1 | `reset()` 竞态（`runGeneration` 进程代数）与终态新建任务语义（方案A） | 完成 | `1307287` | 移除 `isCurrentProcessOrTerminated` 的 `null` 误判，改用单调 `runGeneration` 校验回调；`reset` 仅允许终态任务并复用原请求走 `start` 创建新 `jobId`（非终态返 409、保留原 `checkpoint.pt`） |
| B2 | `launchWorkerForSlotOwner()` 捕获 `RuntimeException` 释放槽位并晋升队列 | 完成 | `2c04439` | 捕获 `IOException \| RuntimeException`，统一转 `failed`（`WORKER_START_FAILED`）、发出合成 `error` 事件、释放槽位并自动晋升排队任务 |
| B3 | 交接文档与 PR 描述补充成员4前端适配清单（合并前置条件） | 完成 | 本次提交 | 新增「合并前置条件：前端适配清单（交给成员4）」与可直接复制的「PR 描述补充」，明确合并顺序约束，未修改 `frontend/` |
| S1 | 接入 `release-slot-on-pause` 配置（暂停释放槽位、恢复重申槽位/排队） | 完成 | `32254a7` | 接入 `TrainingJobStateMachine.occupiesExecutionSlot`：`true` 时确认暂停后释放槽位并晋升排队任务，`resume` 时重申槽位（无空闲槽位返 429 `TRAINING_CAPACITY_EXCEEDED` / `resume_slot_unavailable`） |
| S2 | `jobTimeout` 仅累计实际运行时间（暂停停止计时、恢复续计） | 完成 | `a396323` | 基于单调时钟 `System.nanoTime()` 记录 `remainingJobTimeoutNanos`，`paused` 确认时扣减已运行耗时并取消定时器，`running` 恢复确认后按剩余额度重新计时 |
| S3 | 事件持久化减少锁内 DB 写入与按间隔批量裁剪历史事件 | 完成 | `ff3fafd` | 在 `schedulerLock` 内仅分配单调 `sequence` 与更新内存环形缓冲，`TrainingJobEvent` 插入与批量裁剪移至单任务 `persistenceLock` 锁外执行；流式事件每 16 条才 flush 一次 `TrainingJob`，历史表按溢出阈值 16 条（及终态/按需读取时）批量裁剪 |
| S4 | 恢复 `TrainingJobService.java` 与 `application.yml` 的 `i/crlf` 换行符 | 完成 | `b4b8790` | 消除约 1490 行纯换行符虚增 diff |
| 次要项 | 启动恢复单次执行、真实 `exitCode` 回写、终态内存清理、幂等前置校验、Python 路径冲突提示 | 完成 | `3e0336e` | 移除重复 `@PostConstruct`；`completed/stopped` 确认不再硬编码 `0`，在 `waitForWorkerExit` 写入真实 OS `exitCode`；终态延迟 `terminal-runtime-retention`（默认 `10m`）移除内存 `ManagedJobRuntime` 并全路径回退 H2；幂等查找前置到 `datasetService.getDetail` 之前；补充成员1 `T1-03` Python 路径合并冲突说明 |

## 现状核查（已复核，基于 main @ 32a7d80）

- `backend/spring/.../training/TrainingJobService.java`（1045 行）
  - 任务原保存在内存 `ConcurrentHashMap` 中，服务重启丢失；内部类原名 `TrainingJob`，已在 T3-01 中重命名为 `InMemoryTrainingJob`，为 JPA 实体 `TrainingJob` 让出类名，后续在 T3-03 中统一替换为持久化编排。
  - `start()` 直接用 `ProcessBuilder` 启动 `training_worker.py`，无并发上限、队列和幂等。
  - `pause/resume/stop` 只写 `control.json` 后立即改状态，未等待 worker 确认。
  - `metric` 事件在 `epoch >= totalEpochs` 时过早将状态改为 `completed`（早于 `test_result` 和 `checkpoint.pt` 落盘），将在状态机改造中修正为以 worker `control(completed)` 为准。
- `TrainingCollaborationHandler.java`：握手时调用 `jobService.status(jobId)` 并捕获 `IllegalArgumentException` 关闭非法连接，任务不存在异常需保持向后兼容。
- `backend/python-training/training_worker.py`（1398 行）：`wait_if_paused` 未输出 `paused`/`running` 确认事件，且 batch 内 `stopped` break 后仍会继续后续验证与保存逻辑，将在 T3-08 中一并修复控制循环。
- `frontend/src/app/shared/training/training-runtime.service.ts`：前端通过 `body.message ?? body.error` 解析 HTTP 错误，通过 `status/message/type` 解析控制响应与 WebSocket 事件。

## 协作边界

- 成员1：运行环境与统一配置（含 `T1-03`）。注意本分支在 `backend/spring/src/main/resources/application.yml` 中将 `deepvision.training.python-executable` 硬编码个人路径改为了 `${DEEPVISION_TRAINING_PYTHON:python}`，与成员1的 `T1-03` 修改位置重叠，合并 PR 时请注意解决冲突并以统一的环境变量配置为准（切勿回退为个人绝对路径）。
- 成员2：网络校验、模型构建、协议解析（包括 `training_worker.py` 的模型与协议部分、`TrainingDtos.java` 的网络字段）。本分支只在创建进程前预留 `NetworkValidator` 校验接口，不修改模型构建与协议解析代码。
- 成员4：前端 `frontend/`。本分支不改前端；响应和事件只新增字段，不删除也不重命名已有字段。
- 成员5：测试基础设施与 CI。本分支在 `backend/spring/pom.xml` 中加入了 `<scope>test</scope>` 的 `spring-boot-starter-test`，并新增 `src/test/resources/application-test.yml`（内存 H2 + 临时目录隔离），供成员5统一纳管。

## 设计决策（含原因，按时间追加）

1. **2026-11-14（T3-01 实体与幂等唯一约束）**：新增 `TrainingJob` 实体（表 `training_jobs`），以 `jobId` 为主键，包含 `owner`、`datasetId`、`configDigest`、`status`、`pendingControl`、`createdAt/startedAt/updatedAt/finishedAt`、`lastSequence`、`exitCode`、`errorCode/errorReason`、`device`、`idempotencyKey` 及最新指标快照。为避免 H2 多列唯一索引在 `(owner_username, NULL)` 场景下的潜在冲突，额外维护派生单列唯一键 `idempotencyScopeKey = owner + ":" + idempotencyKey`（无幂等键时为 `null`），既保证无幂等键时同用户可无限提交，又在数据库层严格保证同用户同幂等键唯一、跨用户互不干扰。
2. **2026-11-14（T3-02 状态机与过渡态方案预告）**：主状态固定为 `queued / running / paused / completed / stopped / failed / interrupted` 七种，采用独立的 `pendingControl` 字段（取值 `"pause" | "resume" | "stop" | null`）表达「控制请求已受理、等待 worker 确认」，而不是将 `status` 改为前端未知的 `pausing/stopping` 字符串，以最大化保持与现有 Angular `TrainingRuntimeService` 的向后兼容。
3. **2026-11-14（T3-02 暂停资源策略预告）**：默认 `paused` 状态的任务**继续占用执行槽位**（可通过配置项 `deepvision.training.release-slot-on-pause=false` 控制），原因是暂停期间 Python/PyTorch 子进程仍驻留内存/显存持有模型参数与优化器状态，若释放槽位启动新训练极易导致显存溢出（OOM）。
4. **2026-11-14（T3-04 幂等键传递方式预告）**：选择通过 HTTP 请求头 `Idempotency-Key` 传入幂等键，避免修改与成员2共用的 `StartTrainingRequest` 请求体结构减少合并冲突。
5. **2026-11-14（T3-03 容量调度与可替换进程启动器）**：抽离 `TrainingProcessLauncher` + `ManagedWorkerProcess` 接口，默认使用 `DefaultTrainingProcessLauncher` 启动 OS 子进程，测试中注入 `ControllableTrainingProcessLauncher` 以脱离真实 GPU/Python 完成确定性并发验收；同时将 `application.yml` 中历史遗留的个人绝对路径 `C:/Users/lizihan/...` 清理为 `${DEEPVISION_TRAINING_PYTHON:python}`。
6. **2026-11-14（T3-09 事件单调序号与有界历史存储）**：所有对外事件（包括 worker 输出的 `metric / control / test_result / backprop / error` 以及服务端生成的排队、停止超时、任务超时、非零退出、服务重启 `interrupted` 合成事件）均在 `schedulerLock` 内原子递增 `TrainingJob.lastSequence`（从 `1` 开始单调递增，跨 `reset` 亦不回退）并统一注入 `jobId`、`sequence`、ISO-8601 `timestamp`、`schemaVersion: "1.0"`。事件同时写入内存环形缓冲与 H2 表 `training_job_events`，每次写入自动裁剪保留最近 `max-history-events`（默认 240）条，既支持重启后按 `afterSequence` 增量查询，又防止表无限膨胀。
7. **2026-11-14（B1 进程代数 `runGeneration` 与 `reset` 方案A）**：
   - **进程归属代数（`runGeneration`）**：删除把 `process == null` 当作当前进程的 `isCurrentProcessOrTerminated`；在 `schedulerLock` 内分配槽位时同步递增 `ManagedJobRuntime.runGeneration`，并在启动完成挂载 `process` 时再次在 `schedulerLock` 内核对代数与非终态（若已被取消/强杀则立即销毁子进程）。所有异步回调（`readWorkerOutput`、`waitForWorkerExit`、`onStopTimeout`、`onJobTimeout`）均携带所属 `runGeneration`，代数不一致时直接忽略，彻底杜绝旧进程延迟退出把新运行标为 `failed` 或误释放槽位。
   - **`reset` 语义采用方案A**：经核实前端 `training-runtime.service.ts` 通过 `stop()` 实现重置、未调用 `/reset`，但为保持 REST 接口向后兼容并保护已生成的 `checkpoint.pt`，选择推荐的**方案A**——`POST /api/training/{jobId}/reset` 仅允许在终态（`completed / stopped / failed / interrupted`）调用（非终态返回 HTTP 409 `INVALID_STATE_TRANSITION`），并复用原任务的 `StartTrainingRequest` 委托 `start()` 创建全新 `jobId`（完整执行单用户活动上限、全局并发槽位与队列容量检查），原任务记录及其目录下的 `checkpoint.pt` 保持不变。

8. **2026-11-14（S1 接入 `release-slot-on-pause` 槽位释放与重申策略）**：在 worker 确认 `paused` 时调用 `TrainingJobStateMachine.occupiesExecutionSlot(PAUSED, properties.isReleaseSlotOnPause())`。默认 `false` 时暂停任务继续占用执行槽位以防止 GPU 显存 OOM；当设为 `true`（适合纯 CPU 环境）时，确认暂停后立即释放执行槽位并自动晋升排队任务（此时休眠轮询 `control.json` 的 Python 子进程仍保留在内存中以便恢复，不占执行槽位）；调用 `resume` 时在 `schedulerLock` 内重新申请槽位，若当前无空闲槽位则保持 `paused` 状态并返回明确错误 HTTP 429 `TRAINING_CAPACITY_EXCEEDED`（`details.reason = "resume_slot_unavailable"`），待其他运行任务结束释放槽位后即可成功恢复。
9. **2026-11-14（S2 `jobTimeout` 累计运行时间语义与 30m CPU 默认值评估）**：
   - **超时语义**：`deepvision.training.job-timeout`（`DEEPVISION_TRAINING_JOB_TIMEOUT`）改为**仅累计任务处于 `running` 状态的活跃运行时间**，不计入 `queued` 排队时间与 `paused` 暂停时间。worker 确认 `paused` 时使用 `System.nanoTime()` 扣减本段已运行时间并取消超时定时器；确认恢复 `running` 后按剩余预算重新挂载定时器。
   - **30m 默认值对 CPU 训练的合理性评估**：本项目面向 CNN 教学可视化，内置数据集（`points-2d`、表格 CSV、MNIST 子集约 1k～2k 张 `28x28`、CIFAR-10 子集约 2k～5k 张 `32x32`）及受上传大小上限约束的自定义数据集在主流 4～8 核教学笔记本纯 CPU 环境下，常规 2～3 层卷积 / 20～50 epoch 训练耗时约 5 秒～4 分钟，较深 6 层自定义网络 100 epoch 约 8～15 分钟。在排除课堂演示暂停观察时间后，`30m` 活跃运行上限既为纯 CPU 正常训练留出 2～6 倍安全余量，又能及时回收死循环或卡死进程；超大自定义数据集可通过 `DEEPVISION_TRAINING_JOB_TIMEOUT` 覆盖（如 `60m`）。
10. **2026-11-14（S3 事件锁外持久化、低频实体 flush 与批量历史裁剪）**：将事件处理拆分为两阶段——在全局 `schedulerLock` 内仅原子递增 `TrainingJob.lastSequence`、追加内存环形缓冲，且高频流式事件（`metric`/`backprop`）每累计 `ENTITY_FLUSH_EVENT_INTERVAL = 16` 条才执行一次 `jobRepository.saveAndFlush`（控制/错误/`test_result`/终态事件仍立即 flush）；而 `eventRepository.save` 与历史窗口裁剪全部移出 `schedulerLock`，在单任务 `runtime.persistenceLock` 下执行，并仅当未裁剪溢出达到 `EVENT_PRUNE_BATCH_SIZE = 16` 或进入终态时批量执行 `deleteByJobIdAndSequenceLessThanEqual`。调用 `GET /api/training/{jobId}/events` 时通过 `awaitPendingEventWrites()` 等待在途落盘并按需裁剪精确窗口，既保证 `sequence` 严格单调与读写一致性，又将流式事件在全局锁内的 DB 写入从每条 3 次降至平均 1/16 次。
11. **2026-11-14（次要评审项收敛）**：
    - 移除 `recoverOrphanedJobsOnStartup()` 上的 `@PostConstruct`，仅保留构造函数单次调用；
    - 收到 worker 的 `completed`/`stopped` 控制事件时不再将 `exitCode` 硬编码为 `0`，而是留空（`null`），待 `waitForWorkerExit` 中 `process.waitFor()` 返回后如实写入子进程真实退出码；
    - 新增 `deepvision.training.terminal-runtime-retention`（默认 `10m`），任务进入终态且进程退出后延迟驱逐 `runtimes` 中的 `ManagedJobRuntime`（以及已关闭的 WebSocket 会话集合），驱逐后 `status`、`events`、`reset`、`save`（读取已自动落库的 `TrainingCheckpoint`）与 WebSocket 历史重放全部无缝走 H2 数据库；
    - 将 `start()` 的幂等键查找提前至 `datasetService.getDetail()` 与 `networkValidator.validate()` 之前（同时保留锁内二次校验防并发竞态），保证数据集被删除后客户端使用相同 `Idempotency-Key` 重试仍能幂等返回原任务而非 400。

## 接口与协议变更（给成员2/4/5）

- 新增/变更的 REST 接口：
  - 新增 `GET /api/training/{jobId}/events?afterSequence=0&limit=100`：仅任务所属用户可查询，返回有界历史事件列表 `TrainingEventsResponse`（含 `jobId`、`status`、`schemaVersion`、`lastSequence`、`earliestAvailableSequence`、`truncated`、`events`）；当客户端传入的 `afterSequence` 早于已裁剪窗口时 `truncated=true`。
  - 变更 `POST /api/training/{jobId}/reset`（方案A）：仅允许对终态任务（`completed / stopped / failed / interrupted`）调用，复用原任务配置创建并返回**新 `jobId`**（`TrainingControlResponse.jobId` 为新任务 ID，`status` 为 `running` 或 `queued`，受单用户活动上限与全局并发/队列容量约束）；对非终态任务（`queued / running / paused`）调用返回 HTTP 409 `INVALID_STATE_TRANSITION`（需先 `stop` 进入终态后再 `reset`）。
- 请求头/请求体字段：支持请求头 `X-Request-Id` 透传或自动生成；`POST /api/training/start` 新增可选请求头 `Idempotency-Key`（长度 ≤ 128，同用户同键同配置幂等返回既有任务并将 `idempotentReplay` 置为 `true`；同用户同键但配置 `configDigest` 不同返回 HTTP 409 `IDEMPOTENCY_KEY_CONFLICT`）。
- 响应字段新增（全部向后兼容保留旧字段）：
  - `TrainingStartResponse` 新增：`configDigest`、`idempotentReplay`、`queuePosition`
  - `TrainingStatusResponse` 新增：`pendingControl`、`device`、`lastSequence`、`exitCode`、`errorCode`、`errorReason`、`configDigest`、`createdAt`、`startedAt`、`updatedAt`、`finishedAt`
  - `TrainingControlResponse` 新增：`pendingControl`、`accepted`
  - 新增 `TrainingEventsResponse`：`jobId`、`status`、`schemaVersion`、`lastSequence`、`earliestAvailableSequence`、`truncated`、`events`
- 状态枚举：`queued / running / paused / completed / stopped / failed / interrupted`，配合 `pendingControl`（`pause / resume / stop / null`）表示控制请求已受理待确认；暂停默认继续占用执行槽位。
- 事件字段：所有 WebSocket 推送事件与 `/events` 历史事件在完全保留原有字段（`type / status / message / epoch / batch / loss / accuracy / ...`）基础上，统一包含 `jobId`、`sequence`（每任务从 1 单调递增）、`timestamp`（ISO-8601 UTC 字符串）、`schemaVersion`（`"1.0"`，待与成员2确认）。`control` 与 `error` 事件新增可选 `device`、`errorCode`、`previousStatus`、`queuePosition`。
- 错误码与错误结构（`code/message/details/requestId`，**临时约定，待成员2确认**）：`TrainingErrorResponse` 同时保留旧版 `error` 同名镜像字段，已启用 `INVALID_STATE_TRANSITION`（HTTP 409）、`IDEMPOTENCY_KEY_CONFLICT`（HTTP 409）、`TRAINING_CAPACITY_EXCEEDED`（HTTP 429）、`USER_ACTIVE_JOB_LIMIT_EXCEEDED`（HTTP 429）、`WORKER_START_FAILED`（HTTP 500）。
- 成员2网络校验接入点：`com.deepvision.studio.training.NetworkValidator` 接口（默认由 `TrainingValidationConfig` 提供 `@ConditionalOnMissingBean` 放行实现；成员2只需注册自己的 `NetworkValidator` Bean 即可在创建进程与占用槽位前自动拦截非法网络）。
- 配置项及默认值（`application.yml` 下 `deepvision.training.*`，均支持环境变量覆盖）：
  - `max-concurrent-jobs`: `1`（`DEEPVISION_TRAINING_MAX_CONCURRENT_JOBS`）
  - `max-active-jobs-per-user`: `2`（`DEEPVISION_TRAINING_MAX_ACTIVE_JOBS_PER_USER`）
  - `max-queue-size`: `4`（`DEEPVISION_TRAINING_MAX_QUEUE_SIZE`）
  - `job-timeout`: `30m`（`DEEPVISION_TRAINING_JOB_TIMEOUT`，仅累计 `running` 状态活跃运行时间，`paused` 期间暂停计时）
  - `stop-timeout`: `10s`（`DEEPVISION_TRAINING_STOP_TIMEOUT`）
  - `terminal-runtime-retention`: `10m`（`DEEPVISION_TRAINING_TERMINAL_RUNTIME_RETENTION`，终态任务在内存中保留的时长，到期后自动释放并改走数据库查询）
  - `max-history-events`: `240`（`DEEPVISION_TRAINING_MAX_HISTORY_EVENTS`）
  - `release-slot-on-pause`: `false`（`DEEPVISION_TRAINING_RELEASE_SLOT_ON_PAUSE`）

## 合并前置条件：前端适配清单（交给成员4）

> **合并约束**：本分支未修改 `frontend/`（归属成员4）。由于后端已改为「控制请求受理与 worker 执行确认分离」并新增了 `queued / failed / interrupted` 状态，若直接单独合入 `main`，前端在点击停止后会提前断开 WebSocket 导致界面卡在「运行中」。因此**本 PR 必须与成员4的下列前端适配一起合并，或者在成员4的前端适配之后合并**。

1. **`frontend/src/app/shared/simulation/sim-models.ts`**
   - **`TrainingState` 与 `TrainingRuntimeState`（约第 257～306 行）**：
     - 将 `status` 联合类型从 `'idle' | 'running' | 'paused' | 'stopped' | 'completed'` 扩展为 `'idle' | 'queued' | 'running' | 'paused' | 'stopped' | 'completed' | 'failed' | 'interrupted'`；
     - 新增可选状态详情字段：`pendingControl?: 'pause' | 'resume' | 'stop' | null`、`queuePosition?: number | null`、`errorCode?: string | null`、`errorReason?: string | null`、`device?: string | null`、`lastSequence?: number`。

2. **`frontend/src/app/shared/training/training-runtime.service.ts`**
   - **DTO 接口扩展（`BackendTrainingStartResponse`、`BackendControlResponse`、`BackendMetricMessage`，约第 200～239 行）**：
     - 状态枚举全部补充 `'queued' | 'failed' | 'interrupted'`；
     - `BackendTrainingStartResponse` 增加 `configDigest?: string`、`idempotentReplay?: boolean`、`queuePosition?: number | null`；
     - `BackendControlResponse` 增加 `pendingControl?: 'pause' | 'resume' | 'stop' | null`、`accepted?: boolean`；
     - `BackendMetricMessage` 增加 `sequence?: number`、`timestamp?: string`、`schemaVersion?: string`、`queuePosition?: number | null`、`errorCode?: string | null`、`device?: string | null`；新增 `BackendEventsResponse` 接口映射 `GET /api/training/{jobId}/events`。
   - **`stop()` 与 `reset()`（约第 537～570 行）——停止后保留 WebSocket 等待终态确认**：
     - 在 `stop()` 中调用 `await this.controlBackend('stop', ...)` 后**移除立即执行的 `this.closeSocket()`**（除非响应返回的 `status` 已经是 `'stopped'`，如排队中的任务被直接取消）；保持 WebSocket 连接直到收到终态事件（`control` 事件 `status` 为 `'stopped' | 'completed' | 'interrupted'`，或 `error` 事件进入 `'failed'`）后再关闭 socket。
     - 若 `reset()` 需要等待旧任务停止后再清空会话，建议在确认停止或清理本地状态前妥善处理旧连接，或在终态后按需调用 `POST /api/training/{jobId}/reset` 获取新 `jobId`。
   - **`controlBackend()`（约第 727～740 行）——控制响应仅作为「处理中」提示**：
     - `POST /api/training/{jobId}/pause|resume|stop` 返回的是当前尚未切换的 `status`（如 `'running'`）加上 `pendingControl: 'pause' | 'resume' | 'stop'` 与 `accepted: true`；
     - 不要把该 HTTP 响应当作最终状态切换，而应将 `response.pendingControl` 写入 `state$`（展示「暂停中… / 恢复中… / 停止中…」并禁用重复按钮），待 WebSocket 收到 worker 确认的 `type === 'control'` 事件后再更新最终 `status` 并清空 `pendingControl: null`。
   - **`handleBackendMetric()` 与 `startBackendTraining()`（约第 379～400、633～725 行）——支持新状态与错误详情、禁止提前置 `completed`**：
     - `startBackendTraining()`：当返回 `response.status === 'queued'` 时，写入 `status: 'queued'` 与 `queuePosition: response.queuePosition ?? null`，并正常建立 WebSocket 等待队首自动晋升为 `'running'`；
     - `handleBackendMetric()` 处理 `message.type === 'error'`（第 641～647 行）：将状态置为 `status: 'failed'`（而非原先的 `'stopped'`），保存 `errorCode: message.errorCode ?? null`、`errorReason: message.message ?? null`、`pendingControl: null`，随后关闭 socket；
     - `handleBackendMetric()` 处理 `message.type === 'control'`（第 648～655 行）：同步更新 `status`、`queuePosition`、`device`、`errorCode`（当 `status === 'interrupted'` 时展示重启中断原因），清空已确认的 `pendingControl`，且在 `message.status` 属于 `'completed' | 'stopped' | 'failed' | 'interrupted'` 时关闭 socket；
     - `handleBackendMetric()` 处理 `message.type === 'metric'`（第 698～700 行）：**移除 `status: message.epoch >= message.totalEpochs ? 'completed' : 'running'` 的提前完成判定**（保持 `'running'`，避免在 `test_result` 和 `checkpoint.pt` 生成前过早显示已完成，统一以 `control(completed)` 事件为准）。
   - **断线重连补齐与按 `sequence` 去重（`connectWebSocket()` / `handleBackendMetric()`，约第 608～640 行）**：
     - 为当前 `backendJobId` 维护 `lastSeenSequence = 0`；每次处理 WebSocket 或补拉事件时，若 `typeof event.sequence === 'number'`，当 `event.sequence <= this.lastSeenSequence` 时直接丢弃去重，否则更新 `this.lastSeenSequence = event.sequence`；
     - 当 WebSocket 非预期断开（`onclose` 时本地任务仍处于 `'queued' | 'running' | 'paused'` 非终态），调用 `GET /api/training/${jobId}/events?afterSequence=${this.lastSeenSequence}&limit=240` 按序回放漏掉的事件（若返回 `truncated === true`，额外调用 `GET /api/training/${jobId}/status` 同步最新快照），若返回 `status` 仍非终态则重连 WebSocket。
   - **可选：`startBackendTraining()` 携带幂等键（约第 379～383 行）**：
     - 每次用户点击启动训练生成一次 `const idempotencyKey = crypto.randomUUID()`，通过请求头 `headers: { 'Idempotency-Key': idempotencyKey }` 发送 `POST /api/training/start`；网络超时自动重试同一启动动作时复用该 `idempotencyKey`，避免重复创建训练进程。

3. **`frontend/src/app/modes/mode-b/mode-b-page.component.ts`、`mode-b-page.component.html`（及 `training-collaboration-page.component.ts`）**
   - 状态徽标与提示条支持渲染：
     - `queued`：显示「排队中（队列位置 #`queuePosition`）」，允许点击「停止」直接取消排队任务；
     - `pendingControl` 非空：显示过渡提示（`pause` →「正在等待当前 batch 暂停…」、`resume` →「正在恢复训练…」、`stop` →「正在安全停止训练…」）；
     - `failed`：醒目展示失败状态、`errorCode`（如 `WORKER_CUDA_OOM`、`JOB_TIMEOUT`、`WORKER_NON_ZERO_EXIT`）与后端返回的友好修复建议 `errorReason`；
     - `interrupted`：提示「后端服务重启导致训练中断（不支持断点续训），请重新启动训练」。
   - HTTP 409 / 429 错误提示：利用后端 `TrainingErrorResponse` 返回的 `message`（及 `code`）向用户友好提示并发/队列已满（429）或状态冲突（409）。

## 改动文件清单

- `docs/handoff/member3-training-jobs.md`（更新）
- `backend/spring/pom.xml`（新增 test 作用域 `spring-boot-starter-test`，需成员5知悉）
- `backend/spring/src/test/resources/application-test.yml`（新增，内存 H2 与临时目录测试配置，需成员5知悉）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingJob.java`（新增，JPA 持久化实体）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingJobRepository.java`（新增，JPA Repository）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingJobStatus.java`（新增，7 状态枚举）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingControlAction.java`（新增，控制动作枚举）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingJobStateMachine.java`（新增，集中状态转换表、控制校验与暂停槽位策略）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingApiException.java`（新增，训练模块结构化异常基类）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingStateConflictException.java`（新增，HTTP 409 状态冲突异常）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingCapacityException.java`（新增，HTTP 429 容量限制异常）
- `backend/spring/src/main/java/com/deepvision/studio/training/NetworkValidator.java`（新增，预留给成员2的网络校验接口）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingValidationConfig.java`（新增，`NetworkValidator` 默认放行配置）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingDtos.java`（新增 `TrainingErrorResponse` 及 JSON 未知字段容忍注解）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingController.java`（接入统一异常处理器）
- `backend/spring/src/main/resources/application.yml`（新增训练容量/队列/超时配置项，移除硬编码个人 Python 路径）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingProperties.java`（新增，容量与超时配置绑定）
- `backend/spring/src/main/java/com/deepvision/studio/training/ManagedWorkerProcess.java`（新增，受管 worker 进程接口）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingProcessLauncher.java`（新增，可替换进程启动器接口）
- `backend/spring/src/main/java/com/deepvision/studio/training/DefaultTrainingProcessLauncher.java`（新增，默认 OS 进程启动实现）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingCheckpointRepository.java`（新增按 `jobId` 防重复保存查询）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingJobService.java`（改造为基于 H2 持久化、并发槽位、有界等待队列与 `TrainingProcessLauncher` 的编排服务）
- `backend/spring/src/test/java/com/deepvision/studio/training/ControllableTrainingProcessLauncher.java`（新增，可控模拟 worker 进程启动器）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingTestFixtures.java`（新增，测试数据集与请求构造辅助类）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingCapacityAndQueueTest.java`（新增，AC-03.2 并发/排队/容量限制单测）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingIdempotencyTest.java`（新增，AC-03.1 并发与顺序幂等重放、409 配置冲突及跨用户隔离单测）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingControlConfirmationTest.java`（新增，AC-03.3 控制确认分离、暂停占槽、终态 resume 409 与 stop 超时强杀单测）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingFailureAndRecoveryTest.java`（新增，AC-03.4 & AC-03.5 强杀 worker、启动失败、任务超时与服务重启恢复单测）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingJobRepositoryTest.java`（新增，实体与幂等唯一约束单元测试）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingJobStateMachineTest.java`（新增，集中状态机与 409 校验单元测试）
- `backend/python-training/training_worker.py`（仅改执行与控制部分：启动与控制事件上报 `device`、暂停/恢复/batch 内停止确认修复、CUDA OOM 捕获与退出码 `2`、常规错误退出码 `1`，未触碰模型构建与协议解析）
- `backend/python-training/tests/test_training_worker_control.py`（扩充为 7 项 pytest 单测：覆盖设备选择、暂停/恢复/停止确认、batch 级立即停止、CUDA OOM 提示与退出码、小规模 CPU 训练闭环）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingDeviceAndOomTest.java`（新增，设备落库与 CUDA OOM 状态/退出码传播单测）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingJobEvent.java`（新增，任务有界事件历史 JPA 实体）
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingJobEventRepository.java`（新增，按序号增量查询与窗口裁剪 Repository）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingEventStreamAndHistoryTest.java`（新增，T3-09 事件信封、增量查询、有界裁剪与重启查询单测）
- `backend/spring/src/test/java/com/deepvision/studio/training/FakeWorkerMain.java`（新增，零外部依赖的真实 OS 子进程测试 worker，实现完整 `request.json`/`control.json`/stdout/`checkpoint.pt` 协议）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingEndToEndIntegrationTest.java`（新增，`@SpringBootTest` + `MockMvc` + 真实 OS 子进程端到端验收测试，覆盖 B 模式「启动 → 暂停占槽 → 排队/429 → 停止晋升 → 完成 → 保存 checkpoint → 事件历史查询」与 `NetworkValidator` 接入契约）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingResetAndGenerationTest.java`（新增，B1 回归测试：覆盖非终态 `reset` 409、旧进程延迟退出与尾行输出不污染新运行/槽位、`runGeneration` 代数隔离、原 `checkpoint.pt` 不被覆盖）

## 运行与测试方法

- Java（标准环境）：`cd backend/spring && mvn -q clean test`（期望输出：退出码 `0`，10 个测试类共 **28 项测试全部通过**，0 失败 0 错误）
- Java（本机离线环境命令）：
  ```powershell
  $env:JAVA_HOME = 'D:\Android\jbr'
  mvn -o -f c:\Users\BCA\Desktop\code\CNN-explainer\backend\spring\pom.xml "-Dmaven.repo.local=c:\Users\BCA\Desktop\code\CNN-explainer\backend\spring\.m2\repository" "-DforkCount=0" -q clean test
  ```
- Python：`c:\Users\BCA\Desktop\code\CNN-explainer\backend\python-training\.venv\Scripts\python.exe -m pytest c:\Users\BCA\Desktop\code\CNN-explainer\backend\python-training\tests -q`（期望输出：`7 passed`）
- 实际执行结果记录在下方「验收记录」与「会话日志」中。

## 验收记录

| 验收编号 | 环境与输入 | 操作或命令 | 预期 | 实际 | 证据位置 | 结论 |
| --- | --- | --- | --- | --- | --- | --- |
| AC-03.1 | 内存 H2 + `ControllableTrainingProcessLauncher` 及 `@SpringBootTest` + `MockMvc` 携带 `Idempotency-Key` | `mvn -q test` 执行 `TrainingIdempotencyTest`（8 线程并发栅栏 + 顺序重试 + 异配置冲突）与 `TrainingEndToEndIntegrationTest` | 只有 1 个任务，最多 1 个进程 | 数据库仅创建 1 条 `TrainingJob`，仅启动 1 个进程，所有重试返回相同 `jobId` 且 `idempotentReplay=true`；同键异配置返回 HTTP 409 `IDEMPOTENCY_KEY_CONFLICT` | `TrainingIdempotencyTest.java`, `TrainingEndToEndIntegrationTest.java` | 通过 |
| AC-03.2 | 测试配置：并发 2、队列 3、单用户上限 10，内存 H2 + `ControllableTrainingProcessLauncher` | `mvn -q test` 执行 `TrainingCapacityAndQueueTest` 6 线程栅栏并发提交 | 最多 2 个执行、3 个排队，其余收到容量错误 | 2 个 `running`（启动 2 个受管进程）、3 个 `queued`、1 个抛出 HTTP 429 `TRAINING_CAPACITY_EXCEEDED`；首个任务完成后队首自动晋升为 `running` | `TrainingCapacityAndQueueTest.java` | 通过 |
| AC-03.3 | 内存 H2 + 模拟/真实 OS 子进程 worker | `mvn -q clean test` 执行 `TrainingControlConfirmationTest`、`TrainingResetAndGenerationTest`、`TrainingEndToEndIntegrationTest` 与 `pytest` 控制单测 | 经确认后才变更状态；resume/非终态 reset 返回 409 | 调用 `pause/resume/stop` 后仅写 `control.json` 并标记 `pendingControl`（主状态不变），worker 输出 `control` 事件后才切换为 `paused/running/stopped`；对 `completed` 任务调用 `resume`、对 `running/paused/queued` 任务调用 `reset` 均返回 HTTP 409 `INVALID_STATE_TRANSITION`；超时未确认时强杀并记录 `STOP_TIMEOUT` | `TrainingControlConfirmationTest.java`, `TrainingResetAndGenerationTest.java`, `TrainingEndToEndIntegrationTest.java`, `test_training_worker_control.py` | 通过 |
| AC-03.4 | 内存 H2 + `ControllableTrainingProcessLauncher`（双实例共享同一数据库模拟重启） | `mvn -q clean test` 执行 `TrainingFailureAndRecoveryTest` 与 `TrainingEventStreamAndHistoryTest` | 状态为 failed / interrupted，并有原因 | 强杀 worker 后状态变为 `failed`，记录 `exitCode=137`、`errorCode=WORKER_NON_ZERO_EXIT` 及失败原因；重启后残留 `running/paused/queued` 任务全部变为 `interrupted`（`SERVICE_RESTART_INTERRUPTED`）并记录带序号恢复事件，已完成任务保持 `completed` | `TrainingFailureAndRecoveryTest.java`, `TrainingEventStreamAndHistoryTest.java` | 通过 |
| AC-03.5 | 内存 H2 + 模拟与真实 OS 子进程（覆盖正常完成、stop、reset、强杀、`IOException`/`RuntimeException` 启动失败、超时） | `mvn -q clean test` 执行 `TrainingFailureAndRecoveryTest`、`TrainingControlConfirmationTest`、`TrainingResetAndGenerationTest` 与 `TrainingEndToEndIntegrationTest` | 释放槽位，没有遗留的未知受管进程 | 所有终止与启动异常路径（含 `IOException` 与 `RuntimeException`）均立即置 `failed`（`WORKER_START_FAILED`）、发出合成 `error` 事件、释放 `activeSlotJobIds` 并自动晋升队列下一任务；`stop` 后 `reset` 新建任务及代数替换时旧进程延迟非零退出不会误标新任务失败或误释放槽位（全程 `activeSlotCount <= 上限` 且与 `activeManagedProcessCount` 一致，原 `checkpoint.pt` 保持不变） | `TrainingFailureAndRecoveryTest.java`, `TrainingResetAndGenerationTest.java`, `TrainingEndToEndIntegrationTest.java` | 通过 |

结论只能填：通过 / 失败 / 阻塞 / 未执行。

## 已知问题 / 风险 / 待确认事项

- 错误结构 `{ code, message, details, requestId }` 和 `schemaVersion`（暂定 `"1.0"`）规则为训练模块临时约定，需要与成员2最终对齐。
- 新增的状态值（`queued`、`failed`、`interrupted`）及 `pendingControl` 字段需要与成员4确认前端展示方式。
- `pom.xml` 加入 `spring-boot-starter-test` 及 `application-test.yml` 的方式需要与成员5统一纳管。
- `backend/spring/src/main/resources/application.yml` 中 `deepvision.training.python-executable` 的环境变量化修改与成员1的 `T1-03` 重叠，合并 PR 时需注意解决该行冲突。
- 当前开发机全局 PATH 中未预装 `mvn` 与 `gh`，测试通过本地 JDK (`D:\Android\jbr`) + 临时目录 Apache Maven 3.9.9 与 Python 3.12 (`D:\Python`) 虚拟环境执行。

## PR 描述补充（可直接复制追加到现有 PR 描述）

```markdown
### ⚠️ 合并前置条件（必读）
- **本 PR 必须与成员4的前端适配一起合并，或者在成员4的前端适配之后合并。**
- 原因：本分支按分工边界未修改 `frontend/`。后端已将 `pause / resume / stop` 改为「HTTP 仅返回已受理（`pendingControl`），经 worker WebSocket `control` 事件确认后切换最终状态」，并新增了 `queued / failed / interrupted` 状态；而现有前端 `training-runtime.service.ts` 的 `stop()` 在收到 HTTP 响应后会立即调用 `closeSocket()`，在未合入前端适配前会导致界面收不到后续 `stopped` 确认而停留在运行中。详细逐文件函数级改造说明见 [`docs/handoff/member3-training-jobs.md`](docs/handoff/member3-training-jobs.md) 中的「合并前置条件：前端适配清单（交给成员4）」。

### 代码评审修复摘要（B1–B3、S1–S4 及次要项）
- **B1（`1307287`）**：移除把 `process == null` 误判为当前进程的逻辑，引入在 `schedulerLock` 内随槽位分配单调递增的进程代数 `runGeneration` 校验所有异步 worker 回调；`POST /api/training/{jobId}/reset` 采用方案A——仅允许终态任务调用（非终态返 HTTP 409 `INVALID_STATE_TRANSITION`），复用原请求走 `start()` 容量与单用户上限检查创建新 `jobId`，原任务及 `checkpoint.pt` 保持不变。
- **B2（`2c04439`）**：`launchWorkerForSlotOwner()` 统一捕获 `IOException | RuntimeException`，确保任意启动异常均将任务置为 `failed`（`WORKER_START_FAILED`）、发出合成 `error` 事件、释放执行槽位并自动晋升排队任务。
- **B3（本次提交）**：在交接文档补充成员4前端适配清单（文件、函数、预期行为）及本合并前置条件声明。
- **S1（`32254a7`）**：完整接入 `deepvision.training.release-slot-on-pause`（`TrainingJobStateMachine.occupiesExecutionSlot`），为 `true` 时暂停确认后释放槽位晋升排队任务，`resume` 时重新申请槽位（无槽位返 HTTP 429 `TRAINING_CAPACITY_EXCEEDED` / `resume_slot_unavailable`）。
- **S2（`a396323`）**：`jobTimeout` 改为基于单调时钟仅累计 `running` 状态活跃运行时间（`paused` 期间暂停计时，恢复后按剩余额度续计），并在交接文档补充 30m 默认值对纯 CPU 教学训练的评估。
- **S3（`ff3fafd`）**：将 `TrainingJobEvent` 插入与历史窗口裁剪移出全局 `schedulerLock`（改为单任务 `persistenceLock`），高频流式事件每 16 条才 flush 一次 `TrainingJob`，历史裁剪改为按溢出阈值 16 条（及终态/按需查询时）批量执行，保持 `sequence` 严格单调。
- **S4（`b4b8790`）**：将 `TrainingJobService.java` 与 `application.yml` 恢复为与 `origin/main` 一致的 `i/crlf` 换行符，消除约 1490 行纯换行符 diff。
- **次要项（`3e0336e`）**：移除 `recoverOrphanedJobsOnStartup()` 重复的 `@PostConstruct`；`completed/stopped` 控制事件不再硬编码 `exitCode=0`，改为进程退出时写入真实 OS `exitCode`；新增 `terminal-runtime-retention`（默认 `10m`）延迟清理终态内存对象并全路径回退 H2；幂等查找前置到 `datasetService.getDetail` 之前；补充与成员1 `T1-03` 在 `application.yml` 中 `python-executable` 的合并冲突提示。

### 最新回归测试结果
- **Java（Spring Boot / JUnit 5 / DataJpaTest + MockMvc + 真实 OS 子进程 E2E）**：`mvn -q clean test` —— **10 个测试类共 28 项测试全部通过**（0 失败，0 错误）。
- **Python（PyTorch training worker 控制与 CPU 闭环）**：`pytest tests -q` —— **7 项测试全部通过**。

### 合并冲突提醒
- `backend/spring/src/main/resources/application.yml` 中 `deepvision.training.python-executable` 环境变量化与成员1的 `T1-03` 位置重叠，合并时保留统一的环境变量配置即可。
```

## 下一步（按优先级，下一个 agent 从这里开始）

1. 按用户指示，由用户在终端执行普通推送更新远程分支及 PR（不要加 `--force`）：
   ```powershell
   cd c:\Users\BCA\Desktop\code\CNN-explainer
   git push origin feature/member3-training-job-control
   ```
   推送后将上方「PR 描述补充」章节内容追加到 GitHub PR 描述中。
2. 跨成员联调与合并顺序把控：
   - **成员4（合并前置）**：按本文档「合并前置条件：前端适配清单（交给成员4）」完成 `training-runtime.service.ts`、`sim-models.ts` 与 Mode B 页面适配后一起合并（或先合入前端兼容改动）；
   - **成员1**：合并 `application.yml` 时对齐 `deepvision.training.python-executable`（`T1-03`）；
   - **成员2**：对齐 `TrainingErrorResponse`（`code/message/details/requestId`）全局收敛及 `NetworkValidator` Bean 接入；
   - **成员5**：对齐 `backend/spring/pom.xml` 测试依赖与 CI 流水线执行 `mvn -q clean test` + `pytest`。

## 会话日志

- 2026-11-14 13:37：初始化。从 origin/main 建立分支，创建本交接文档，还没有改代码。
- 2026-11-14 14:44（member3-agent）：完成现状核查与 T3-01（`TrainingJob` 实体、`TrainingJobRepository`、`spring-boot-starter-test`、`application-test.yml` 及 `TrainingJobRepositoryTest`，`mvn test` 与 `pytest` 均通过，提交 `3e154ba`）。
- 2026-11-14 14:55（member3-agent）：完成 T3-02（`TrainingJobStatus`、`TrainingControlAction`、`TrainingJobStateMachine` 集中转换表与暂停占槽策略、统一错误契约 `TrainingErrorResponse`、`NetworkValidator` 扩展接口及单测，提交 `4249500`）。
- 2026-11-14 15:08（member3-agent）：完成 T3-03（`TrainingProperties`、`TrainingProcessLauncher`、并发槽位与有界队列调度、`TrainingCapacityAndQueueTest` 验收 AC-03.2，`mvn test` 与 `pytest` 均通过，提交 `4760840`）。
- 2026-11-14 15:11（member3-agent）：完成 T3-04（`Idempotency-Key` 幂等启动、配置摘要冲突 409 拦截与 `TrainingIdempotencyTest` 验收 AC-03.1，`mvn test` 与 `pytest` 均通过，提交 `082b725`）。
- 2026-11-14 15:15（member3-agent）：完成 T3-05（控制请求受理与 worker 执行确认分离、`stopTimeout` 强杀兜底、终态 `resume` 409 校验与 `TrainingControlConfirmationTest` 验收 AC-03.3，`mvn test` 与 `pytest` 均通过，提交 `0a4d9bf`）。
- 2026-11-14 15:19（member3-agent）：完成 T3-06 & T3-07（全路径异常清理、槽位回收、重启恢复 `interrupted` 与 `TrainingFailureAndRecoveryTest` 验收 AC-03.4/AC-03.5，`mvn test` 与 `pytest` 均通过，提交 `24d0730`）。
- 2026-11-14 15:26（member3-agent）：完成 T3-08（`training_worker.py` 设备上报、控制确认事件、CUDA OOM 友好提示与退出码，消除 Java stdout 与 `waitFor` 尾行竞态，`mvn test` 15 项与 `pytest` 7 项全部通过，提交 `83339a2`）。
- 2026-11-14 15:42（member3-agent）：完成 T3-09（对外事件统一补齐 `jobId/sequence/timestamp/schemaVersion`、`TrainingJobEvent` 有界历史持久化、`GET /api/training/{jobId}/events` 增量查询与 `TrainingEventStreamAndHistoryTest`，`mvn test` 17 项与 `pytest` 7 项全部通过，提交 `0b311c4`）。
- 2026-11-14 15:50（member3-agent）：新增真实 OS 子进程 `FakeWorkerMain` 与 `TrainingEndToEndIntegrationTest`（`@SpringBootTest` + `MockMvc` 覆盖 B 模式完整训练保存闭环、HTTP 409/429/422 错误契约、`NetworkValidator` 钩子与进程回收），全套 19 项 Java 测试与 7 项 Python pytest 测试全部通过，提交 `655b698`。
- 2026-11-14 16:00（member3-agent）：执行 `git push -u origin feature/member3-training-job-control` 时因后台非交互环境无法弹出 GitHub HTTPS 凭据认证而阻塞，且本机无 `gh` CLI；按第 6 节规范取消挂起命令、记录状态并提供手动推送与创建 PR 命令。
- 2026-11-14 16:17（member3-followup-agent）：开始处理代码评审意见；首先完成 S4，将 `TrainingJobService.java` 与 `application.yml` 恢复为与 `origin/main` 一致的 `i/crlf`（两文件相对 `origin/main` 的 diff 从 `2037+/1090-` 降至 `1292+/345-`），并执行 `mvn -o "-Dmaven.repo.local=.m2/repository" "-DforkCount=0" -q clean test`（19 项通过）与 `pytest tests -q`（7 项通过），提交 `b4b8790`。
- 2026-11-14 16:33（member3-followup-agent）：完成 B1（引入 `runGeneration` 进程代数消除旧进程回调竞态，`reset()` 采用方案A仅允许终态任务创建新 `jobId` 并保护原 `checkpoint.pt`，新增 `TrainingResetAndGenerationTest` 3 项回归测试），执行 `JAVA_HOME=D:\Android\jbr mvn -o "-Dmaven.repo.local=.m2/repository" "-DforkCount=0" -q clean test`（22 项通过）与 `.venv\Scripts\python.exe -m pytest tests -q`（7 项通过），提交 `1307287`。
- 2026-11-14 16:35（member3-followup-agent）：完成 B2（`launchWorkerForSlotOwner()` 捕获 `IOException | RuntimeException`，确保任意启动异常均标记 `failed`/`WORKER_START_FAILED`、发出合成 `error` 事件、释放槽位并晋升下一排队任务，在 `TrainingFailureAndRecoveryTest` 新增回归测试），执行 `JAVA_HOME=D:\Android\jbr mvn -o "-Dmaven.repo.local=.m2/repository" "-DforkCount=0" -q clean test`（23 项通过）与 `.venv\Scripts\python.exe -m pytest tests -q`（7 项通过），提交 `2c04439`。
- 2026-11-14 16:38（member3-followup-agent）：完成 S1（接入 `deepvision.training.release-slot-on-pause`，支持暂停确认释放槽位晋升排队任务、恢复时重申槽位及无槽位 429 错误，在 `TrainingControlConfirmationTest` 补充单测），执行 `JAVA_HOME=D:\Android\jbr mvn -o "-Dmaven.repo.local=.m2/repository" "-DforkCount=0" -q clean test`（24 项通过）与 `.venv\Scripts\python.exe -m pytest tests -q`（7 项通过），提交 `32254a7`。
- 2026-11-14 16:40（member3-followup-agent）：完成 S2（将 `jobTimeout` 改为基于单调时钟仅累计 `running` 活跃耗时、`paused` 停止计时并在文档中评估 30m CPU 默认值合理性，在 `TrainingFailureAndRecoveryTest` 补充暂停跨超时单测），执行 `JAVA_HOME=D:\Android\jbr mvn -o "-Dmaven.repo.local=.m2/repository" "-DforkCount=0" -q clean test`（25 项通过）与 `.venv\Scripts\python.exe -m pytest tests -q`（7 项通过），提交 `a396323`。
- 2026-11-14 16:50（member3-followup-agent）：完成 S3（将 `TrainingJobEvent` 写入与裁剪移出全局 `schedulerLock`、流式事件每 16 条批量 flush `TrainingJob`、历史事件按阈值 16 批量裁剪并在读取/终态兜底对齐窗口，在 `TrainingEventStreamAndHistoryTest` 补充批量裁剪单测），执行 `JAVA_HOME=D:\Android\jbr mvn -o "-Dmaven.repo.local=.m2/repository" "-DforkCount=0" -q clean test`（26 项通过）与 `.venv\Scripts\python.exe -m pytest tests -q`（7 项通过），提交 `ff3fafd`。
- 2026-11-14 16:57（member3-followup-agent）：完成 5 项次要评审项（移除重复 `@PostConstruct`、在 `waitForWorkerExit` 写入真实 OS `exitCode`、新增 `terminal-runtime-retention` 延迟驱逐终态内存对象并回退 H2、将幂等查找前置到 `datasetService.getDetail` 之前、补充成员1 `T1-03` Python 路径冲突提示，并在 `TrainingControlConfirmationTest` 与 `TrainingIdempotencyTest` 新增单测），执行 `JAVA_HOME=D:\Android\jbr mvn -o "-Dmaven.repo.local=.m2/repository" "-DforkCount=0" -q clean test`（28 项通过）与 `.venv\Scripts\python.exe -m pytest tests -q`（7 项通过），提交 `3e0336e`。
- 2026-11-14 17:00（member3-followup-agent）：完成 B3（在交接文档中补充逐文件函数级的「合并前置条件：前端适配清单（交给成员4）」与可直接复制粘贴的「PR 描述补充」），再次复核全套 28 项 Java 测试与 7 项 Python 测试通过、`frontend/` 零改动、`i/crlf` 换行符一致；按用户指示不执行 `git push`，交由用户手动推送更新远程分支。
