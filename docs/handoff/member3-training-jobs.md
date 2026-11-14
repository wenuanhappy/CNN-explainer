# 成员3 训练任务与资源控制：Agent 交接文档

> 维护规则：每个 agent 开始工作前先完整阅读本文档；每完成一个任务或一次提交后，立即更新本文档并与代码放在同一个提交中；结束会话前必须更新「下一步」和「会话日志」。

## 基本信息

- 分支：`feature/member3-training-job-control`（基于 `origin/main` @ `32a7d80`）
- 最近更新：2026-11-14 16:00 by member3-agent
- 当前状态：待评审（本地分支已完成 T3-01 ~ T3-09 与 AC-03.1 ~ AC-03.5 全部提交并通过 19 项 Java + 7 项 Python 测试；后台非交互 `git push` 等待 GitHub HTTPS 凭据弹窗阻塞且本机无 `gh` CLI，已按规范停止并附上手动推送与提 PR 命令）
- PR：待手动推送分支并创建（见下方「下一步」）
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
| B3 | 交接文档与 PR 描述补充成员4前端适配清单（合并前置条件） | 未开始 | | |
| S1 | 接入 `release-slot-on-pause` 配置（暂停释放槽位、恢复重申槽位/排队） | 完成 | 本次提交 | 接入 `TrainingJobStateMachine.occupiesExecutionSlot`：`true` 时确认暂停后释放槽位并晋升排队任务，`resume` 时重申槽位（无空闲槽位返 429 `TRAINING_CAPACITY_EXCEEDED` / `resume_slot_unavailable`） |
| S2 | `jobTimeout` 仅累计实际运行时间（暂停停止计时、恢复续计） | 未开始 | | |
| S3 | 事件持久化减少锁内 DB 写入与按间隔批量裁剪历史事件 | 未开始 | | |
| S4 | 恢复 `TrainingJobService.java` 与 `application.yml` 的 `i/crlf` 换行符 | 完成 | `b4b8790` | 消除约 1490 行纯换行符虚增 diff |
| 次要项 | 启动恢复单次执行、真实 `exitCode` 回写、终态内存清理、幂等前置校验、Python 路径冲突提示 | 未开始 | | |

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
  - `job-timeout`: `30m`（`DEEPVISION_TRAINING_JOB_TIMEOUT`）
  - `stop-timeout`: `10s`（`DEEPVISION_TRAINING_STOP_TIMEOUT`）
  - `max-history-events`: `240`（`DEEPVISION_TRAINING_MAX_HISTORY_EVENTS`）
  - `release-slot-on-pause`: `false`（`DEEPVISION_TRAINING_RELEASE_SLOT_ON_PAUSE`）

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

- Java（标准环境）：`cd backend/spring && mvn -q test`（期望输出：退出码 `0`，19 项测试全部通过，0 失败 0 错误）
- Python：`cd backend/python-training && .\.venv\Scripts\python.exe -m pytest tests -q`（期望输出：`7 passed`）
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
- 当前开发机全局 PATH 中未预装 `mvn` 与 `gh`，测试通过本地 JDK (`D:\Android\jbr`) + 临时目录 Apache Maven 3.9.9 与 Python 3.12 (`D:\Python`) 虚拟环境执行。

## 下一步（按优先级，下一个 agent 从这里开始）

1. 在交互式终端手动完成分支推送与创建 PR（因仓库为公开仓库，`git fetch` 无需认证，而后台非交互终端执行 `git push` 时 Git Credential Manager 等待交互式 GitHub 凭据导致挂起，且本机未安装 `gh` CLI；按规范不绕过权限）：
   ```powershell
   cd c:\Users\BCA\Desktop\code\CNN-explainer
   git push -u origin feature/member3-training-job-control
   # 若已安装并登录 gh CLI：
   gh pr create --base main --head feature/member3-training-job-control --title "feat(training): implement P0 training job lifecycle, resource limits, and recovery (T3-01..T3-09)"
   ```
   推送成功后也可直接打开：`https://github.com/CiaCrystal/CNN-explainer/compare/main...feature/member3-training-job-control?expand=1` 粘贴 PR 描述提交。
2. 代码评审与联调对齐：
   - 与成员2对齐 `TrainingErrorResponse`（`code/message/details/requestId`）全局收敛及 `NetworkValidator` Bean 接入；
   - 与成员4对齐 `queued / failed / interrupted` 状态、`pendingControl` 过渡指示、`Idempotency-Key` 请求头与 `/events?afterSequence=N` 增量拉取；
   - 与成员5对齐 `backend/spring/pom.xml` 测试依赖与 CI 流水线执行 `mvn -q test` + `pytest`。

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
- 2026-11-14 16:38（member3-followup-agent）：完成 S1（接入 `deepvision.training.release-slot-on-pause`，支持暂停确认释放槽位晋升排队任务、恢复时重申槽位及无槽位 429 错误，在 `TrainingControlConfirmationTest` 补充单测），执行 `JAVA_HOME=D:\Android\jbr mvn -o "-Dmaven.repo.local=.m2/repository" "-DforkCount=0" -q clean test`（24 项通过）与 `.venv\Scripts\python.exe -m pytest tests -q`（7 项通过）。
