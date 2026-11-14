# 成员3 训练任务与资源控制：Agent 交接文档

> 维护规则：每个 agent 开始工作前先完整阅读本文档；每完成一个任务或一次提交后，立即更新本文档并与代码放在同一个提交中；结束会话前必须更新「下一步」和「会话日志」。

## 基本信息

- 分支：`feature/member3-training-job-control`（基于 `origin/main` @ `32a7d80`）
- 最近更新：2026-11-14 14:55 by member3-agent
- 当前状态：进行中（已完成 T3-01、T3-02，正在推进 T3-03 ~ T3-09）
- PR：未创建
- 任务依据：《DeepVision Studio 五人小组任务分工与验收安排》第 6 节（成员3），只做 P0，不做 P1

## 任务进度

| 编号 | 内容 | 状态 | 相关提交 | 备注 |
| --- | --- | --- | --- | --- |
| T3-01 | 持久化任务（实体 + Repository） | 完成 | `3e154ba` | `TrainingJob` + `TrainingJobRepository` + `application-test.yml` |
| T3-02 | 状态机与转换规则、暂停资源策略 | 完成 | 本次提交 | 集中转换表 `TrainingJobStateMachine` + `pendingControl` + 409 错误契约 + `NetworkValidator` 预留 |
| T3-03 | 并发 / 用户活动数 / 队列长度 / 超时配置 | 未开始 | | 本地默认并发 1 |
| T3-04 | 幂等启动 | 未开始 | | 拟使用请求头 `Idempotency-Key` |
| T3-05 | 控制受理与 worker 执行确认分离 | 未开始 | | 终态 resume 返回 409 |
| T3-06 | 启动失败、非零退出、停止超时、残留进程、槽位释放 | 未开始 | | |
| T3-07 | 重启后未知活动任务标记 interrupted | 未开始 | | 不做精确续训 |
| T3-08 | 实际设备上报、友好的 OOM 提示、CPU 路径 | 未开始 | | |
| T3-09 | 事件 `jobId/sequence/timestamp/schemaVersion`，以及状态和历史查询 | 未开始 | | |

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

## 接口与协议变更（给成员2/4/5）

- 新增/变更的 REST 接口：（T3-09 将新增 `GET /api/training/{jobId}/events`）
- 请求头/请求体字段：支持请求头 `X-Request-Id` 透传或自动生成；（T3-04 将支持 `POST /api/training/start` 可选请求头 `Idempotency-Key`）
- 状态枚举：`queued / running / paused / completed / stopped / failed / interrupted`，配合 `pendingControl`（`pause / resume / stop / null`）表示控制请求已受理待确认；暂停默认继续占用执行槽位。
- 事件字段：（T3-09 将统一补齐 `jobId / sequence / timestamp / schemaVersion`）
- 错误码与错误结构（`code/message/details/requestId`，**临时约定，待成员2确认**）：`TrainingErrorResponse` 同时保留旧版 `error` 同名镜像字段，新增 `INVALID_STATE_TRANSITION`（HTTP 409）等结构化异常。
- 成员2网络校验接入点：`com.deepvision.studio.training.NetworkValidator` 接口（默认由 `TrainingValidationConfig` 提供 `@ConditionalOnMissingBean` 放行实现；成员2只需注册自己的 `NetworkValidator` Bean 即可在创建进程与占用槽位前自动拦截非法网络）。
- 配置项及默认值：（T3-03 引入；测试环境配置见 `backend/spring/src/test/resources/application-test.yml`）

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
- `backend/spring/src/main/java/com/deepvision/studio/training/TrainingJobService.java`（重命名私有内部类消除命名冲突）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingJobRepositoryTest.java`（新增，实体与幂等唯一约束单元测试）
- `backend/spring/src/test/java/com/deepvision/studio/training/TrainingJobStateMachineTest.java`（新增，集中状态机与 409 校验单元测试）
- `backend/python-training/tests/test_training_worker_control.py`（新增，worker 控制文件读取基础单测）

## 运行与测试方法

- Java：`cd backend/spring; mvn -q test`（当前本地环境中 Maven 安装于会话临时目录，使用 OpenJDK 21 按 Java 17 目标编译运行）
- Python：`cd backend/python-training; .\.venv\Scripts\python.exe -m pytest tests -q`
- 实际执行结果记录在下方「验收记录」与「会话日志」中。

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

- 错误结构 `{ code, message, details, requestId }` 和 `schemaVersion`（暂定 `"1.0"`）规则为训练模块临时约定，需要与成员2最终对齐。
- 新增的状态值（`queued`、`failed`、`interrupted`）及 `pendingControl` 字段需要与成员4确认前端展示方式。
- `pom.xml` 加入 `spring-boot-starter-test` 及 `application-test.yml` 的方式需要与成员5统一纳管。
- 当前开发机全局 PATH 中未预装 `mvn` 与 `gh`，测试通过本地 JDK (`D:\Android\jbr`) + 临时目录 Apache Maven 3.9.9 与 Python 3.12 (`D:\Python`) 虚拟环境执行。

## 下一步（按优先级，下一个 agent 从这里开始）

1. 实现 T3-03：`TrainingProperties` 容量与超时配置、`TrainingProcessLauncher` 抽象、全局并发槽位 + 用户活动任务数上限 + 有界等待队列，将 `TrainingJobService` 切换到持久化实体与队列调度，覆盖 AC-03.2。
2. 实现 T3-04 ~ T3-07：幂等启动、控制受理与确认分离、启动失败/非零退出/停止超时/任务超时/残留进程清理、重启恢复 `interrupted`。
3. 实现 T3-08 ~ T3-09 及全链路集成测试。

## 会话日志

- 2026-11-14 13:37：初始化。从 origin/main 建立分支，创建本交接文档，还没有改代码。
- 2026-11-14 14:44（member3-agent）：完成现状核查与 T3-01（`TrainingJob` 实体、`TrainingJobRepository`、`spring-boot-starter-test`、`application-test.yml` 及 `TrainingJobRepositoryTest`，`mvn test` 与 `pytest` 均通过，提交 `3e154ba`）。
- 2026-11-14 14:55（member3-agent）：完成 T3-02（`TrainingJobStatus`、`TrainingControlAction`、`TrainingJobStateMachine` 集中转换表与暂停占槽策略、统一错误契约 `TrainingErrorResponse`、`NetworkValidator` 扩展接口及单测）。
