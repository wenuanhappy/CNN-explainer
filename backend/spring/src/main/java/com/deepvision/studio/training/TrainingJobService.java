package com.deepvision.studio.training;

import com.deepvision.studio.auth.AppUser;
import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.TrainingDtos.CheckpointTestResult;
import com.deepvision.studio.training.TrainingDtos.HistogramBin;
import com.deepvision.studio.training.TrainingDtos.InferenceSampleListResponse;
import com.deepvision.studio.training.TrainingDtos.SingleInferenceRequest;
import com.deepvision.studio.training.TrainingDtos.SingleInferenceResult;
import com.deepvision.studio.training.TrainingDtos.SplitRequest;
import com.deepvision.studio.training.TrainingDtos.StartTrainingRequest;
import com.deepvision.studio.training.TrainingDtos.TestCheckpointRequest;
import com.deepvision.studio.training.TrainingDtos.TrainingCheckpointSummary;
import com.deepvision.studio.training.TrainingDtos.TrainingConfigRequest;
import com.deepvision.studio.training.TrainingDtos.TrainingControlResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingDatasetDetail;
import com.deepvision.studio.training.TrainingDtos.TrainingMetricMessage;
import com.deepvision.studio.training.TrainingDtos.TrainingStartResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingStatusResponse;
import com.deepvision.studio.training.TrainingDtos.WeightHistogramResponse;
import com.deepvision.studio.training.TrainingProcessLauncher.WorkerLaunchRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Service
public class TrainingJobService {
  private static final DateTimeFormatter JOB_ID_TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
  private static final int MAX_CHECKPOINT_METRIC_HISTORY = 2000;

  private final TrainingDatasetService datasetService;
  private final AppUserRepository users;
  private final TrainingCheckpointRepository checkpoints;
  private final TrainingJobRepository jobRepository;
  private final NetworkValidator networkValidator;
  private final TrainingProcessLauncher processLauncher;
  private final TrainingProperties properties;
  private final ObjectMapper objectMapper;

  private final ExecutorService workerIoExecutor = Executors.newCachedThreadPool();
  private final ScheduledExecutorService timerExecutor = Executors.newScheduledThreadPool(2);
  private final Object schedulerLock = new Object();

  private final Map<String, ManagedJobRuntime> runtimes = new ConcurrentHashMap<>();
  private final Set<String> activeSlotJobIds = new LinkedHashSet<>();
  private final ArrayDeque<String> waitQueue = new ArrayDeque<>();
  private final Map<String, CopyOnWriteArraySet<WebSocketSession>> sessions = new ConcurrentHashMap<>();

  private final String streamBaseUrl;
  private final Path datasetRoot;
  private final Path jobRoot;
  private final Path workerScript;
  private final String pythonExecutable;
  private volatile boolean shuttingDown = false;

  public TrainingJobService(
      TrainingDatasetService datasetService,
      AppUserRepository users,
      TrainingCheckpointRepository checkpoints,
      TrainingJobRepository jobRepository,
      NetworkValidator networkValidator,
      TrainingProcessLauncher processLauncher,
      TrainingProperties properties,
      ObjectMapper objectMapper,
      @Value("${deepvision.datasets.root}") String datasetRoot
  ) {
    this.datasetService = datasetService;
    this.users = users;
    this.checkpoints = checkpoints;
    this.jobRepository = jobRepository;
    this.networkValidator = networkValidator;
    this.processLauncher = processLauncher;
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.streamBaseUrl = trimTrailingSlash(properties.getStreamBaseUrl());
    this.datasetRoot = Path.of(datasetRoot).toAbsolutePath().normalize();
    this.jobRoot = Path.of(properties.getJobsRoot()).toAbsolutePath().normalize();
    this.pythonExecutable = properties.getPythonExecutable();
    this.workerScript = Path.of(properties.getWorkerScript()).toAbsolutePath().normalize();
  }

  public TrainingStartResponse start(StartTrainingRequest request, String username) {
    requireUser(username);
    TrainingDatasetDetail dataset = datasetService.getDetail(request.datasetId(), username);
    validateSplit(request.split());
    if (!dataset.hasLabels()) {
      throw new IllegalArgumentException("Dataset has no labels and cannot be used for supervised training.");
    }
    networkValidator.validate(request, dataset);

    TrainingConfigRequest config = request.config();
    int totalEpochs = valueOrDefault(config.totalEpochs(), 20);
    int batchSize = valueOrDefault(config.batchSize(), 32);
    double trainSamples = Math.max(1, dataset.sampleCount() * request.split().train());
    int totalBatches = Math.max(1, (int) Math.ceil(trainSamples / batchSize));
    String modelSig = modelSignature(request);
    String configDigest = computeConfigDigest(request);
    String requestJson = serializeRequest(request);

    ManagedJobRuntime runtimeToLaunch = null;
    Integer queuePosition = null;
    String initialStatus;
    String jobId;

    synchronized (schedulerLock) {
      long userActiveCount = jobRepository.countByOwnerAndStatusIn(username, TrainingJobStatus.activeWireValues());
      int maxUserActive = properties.getMaxActiveJobsPerUser();
      if (userActiveCount >= maxUserActive) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("reason", "user_active_limit_exceeded");
        details.put("maxActiveJobsPerUser", maxUserActive);
        details.put("currentUserActiveJobs", userActiveCount);
        throw new TrainingCapacityException(
            "USER_ACTIVE_JOB_LIMIT_EXCEEDED",
            "Active training job limit per user (" + maxUserActive + ") exceeded.",
            details
        );
      }

      int maxConcurrent = properties.getMaxConcurrentJobs();
      int maxQueue = properties.getMaxQueueSize();
      boolean hasSlot = activeSlotJobIds.size() < maxConcurrent;
      if (!hasSlot && waitQueue.size() >= maxQueue) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("reason", "queue_full");
        details.put("maxConcurrentJobs", maxConcurrent);
        details.put("activeSlots", activeSlotJobIds.size());
        details.put("maxQueueSize", maxQueue);
        details.put("queuedJobs", waitQueue.size());
        throw new TrainingCapacityException(
            "TRAINING_CAPACITY_EXCEEDED",
            "Training execution slots (" + maxConcurrent + ") and queue (" + maxQueue + ") are full.",
            details
        );
      }

      jobId = nextJobId();
      Instant now = Instant.now();
      TrainingJobStatus targetStatus = hasSlot ? TrainingJobStatus.RUNNING : TrainingJobStatus.QUEUED;
      initialStatus = targetStatus.wireValue();

      TrainingJob entity = new TrainingJob(
          jobId,
          username,
          request.datasetId(),
          configDigest,
          modelSig,
          initialStatus,
          null,
          totalEpochs,
          totalBatches,
          requestJson,
          now
      );
      if (hasSlot) {
        entity.setStartedAt(now);
      }
      jobRepository.saveAndFlush(entity);

      ManagedJobRuntime runtime = new ManagedJobRuntime(entity, request);
      runtimes.put(jobId, runtime);

      if (hasSlot) {
        activeSlotJobIds.add(jobId);
        runtimeToLaunch = runtime;
      } else {
        waitQueue.addLast(jobId);
        queuePosition = waitQueue.size();
      }
    }

    if (runtimeToLaunch != null) {
      launchWorkerForSlotOwner(runtimeToLaunch, true);
    }

    return new TrainingStartResponse(
        jobId,
        initialStatus,
        totalEpochs,
        totalBatches,
        streamUrl(jobId),
        configDigest,
        false,
        queuePosition
    );
  }

  public TrainingStatusResponse status(String jobId) {
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      return runtime.toStatusResponse();
    }
    TrainingJob entity = jobRepository.findById(jobId)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    return toStatusResponse(entity);
  }

  public TrainingStatusResponse status(String username, String jobId) {
    requireUser(username);
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      if (!username.equals(runtime.owner())) {
        throw new IllegalArgumentException("Training job not found.");
      }
      return runtime.toStatusResponse();
    }
    TrainingJob entity = jobRepository.findByJobIdAndOwner(jobId, username)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    return toStatusResponse(entity);
  }

  public WeightHistogramResponse histogram(String jobId) {
    return histogram(requireRuntime(jobId));
  }

  public WeightHistogramResponse histogram(String username, String jobId) {
    return histogram(requireOwnedRuntime(username, jobId));
  }

  private WeightHistogramResponse histogram(ManagedJobRuntime runtime) {
    TrainingMetricMessage metric = runtime.latestMetric();
    double mean = metric == null ? 0 : metric.weightMean();
    double std = metric == null ? 0.16 : Math.max(0.02, metric.weightStd());
    List<HistogramBin> bins = new ArrayList<>();
    for (int i = -5; i <= 5; i += 1) {
      double value = mean + i * std * 0.35;
      int count = (int) Math.round(80 * Math.exp(-0.5 * Math.pow(i / 2.1, 2))) + Math.max(0, runtime.epoch());
      bins.add(new HistogramBin(String.format(java.util.Locale.US, "%.2f", value), count));
    }
    return new WeightHistogramResponse(runtime.jobId(), runtime.epoch(), bins);
  }

  public TrainingControlResponse pause(String jobId) {
    return pause(requireRuntime(jobId));
  }

  public TrainingControlResponse pause(String username, String jobId) {
    return pause(requireOwnedRuntime(username, jobId));
  }

  private TrainingControlResponse pause(ManagedJobRuntime runtime) {
    synchronized (schedulerLock) {
      TrainingJobStateMachine.validateControlRequest(
          runtime.jobId(), runtime.statusEnum(), runtime.pendingControlEnum(), TrainingControlAction.PAUSE
      );
      writeControl(runtime, "paused");
      transitionJobLocked(runtime, TrainingJobStatus.PAUSED, null, null, null);
    }
    return new TrainingControlResponse(runtime.jobId(), runtime.status(), "Training paused.", null, true);
  }

  public TrainingControlResponse resume(String jobId) {
    return resume(requireRuntime(jobId));
  }

  public TrainingControlResponse resume(String username, String jobId) {
    return resume(requireOwnedRuntime(username, jobId));
  }

  private TrainingControlResponse resume(ManagedJobRuntime runtime) {
    synchronized (schedulerLock) {
      TrainingJobStateMachine.validateControlRequest(
          runtime.jobId(), runtime.statusEnum(), runtime.pendingControlEnum(), TrainingControlAction.RESUME
      );
      writeControl(runtime, "running");
      transitionJobLocked(runtime, TrainingJobStatus.RUNNING, null, null, null);
    }
    return new TrainingControlResponse(runtime.jobId(), runtime.status(), "Training resumed.", null, true);
  }

  public TrainingControlResponse stop(String jobId) {
    return stop(requireRuntime(jobId));
  }

  public TrainingControlResponse stop(String username, String jobId) {
    return stop(requireOwnedRuntime(username, jobId));
  }

  private TrainingControlResponse stop(ManagedJobRuntime runtime) {
    List<ManagedJobRuntime> promoted = new ArrayList<>();
    synchronized (schedulerLock) {
      TrainingJobStateMachine.validateControlRequest(
          runtime.jobId(), runtime.statusEnum(), runtime.pendingControlEnum(), TrainingControlAction.STOP
      );
      if (runtime.statusEnum() == TrainingJobStatus.QUEUED) {
        waitQueue.remove(runtime.jobId());
        transitionJobLocked(runtime, TrainingJobStatus.STOPPED, null, null, "Stopped while queued.");
        return new TrainingControlResponse(runtime.jobId(), runtime.status(), "Training stopped.", null, true);
      }
      writeControl(runtime, "stopped");
      runtime.destroyProcessForcibly();
      transitionJobLocked(runtime, TrainingJobStatus.STOPPED, null, null, null);
      releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
    }
    launchPromotedRuntimes(promoted);
    return new TrainingControlResponse(runtime.jobId(), runtime.status(), "Training stopped.", null, true);
  }

  public TrainingControlResponse reset(String jobId) {
    return reset(requireRuntime(jobId));
  }

  public TrainingControlResponse reset(String username, String jobId) {
    return reset(requireOwnedRuntime(username, jobId));
  }

  private TrainingControlResponse reset(ManagedJobRuntime runtime) {
    boolean launchNow = false;
    synchronized (schedulerLock) {
      writeControl(runtime, "stopped");
      runtime.destroyProcessForcibly();
      waitQueue.remove(runtime.jobId());
      boolean alreadyHasSlot = activeSlotJobIds.contains(runtime.jobId());
      if (!alreadyHasSlot && activeSlotJobIds.size() >= properties.getMaxConcurrentJobs()) {
        if (waitQueue.size() >= properties.getMaxQueueSize()) {
          throw new TrainingCapacityException(
              "TRAINING_CAPACITY_EXCEEDED",
              "No capacity available to reset training job.",
              Map.of("jobId", runtime.jobId())
          );
        }
        runtime.resetForRerun(TrainingJobStatus.QUEUED, null);
        jobRepository.saveAndFlush(runtime.entity());
        waitQueue.addLast(runtime.jobId());
      } else {
        activeSlotJobIds.add(runtime.jobId());
        runtime.resetForRerun(TrainingJobStatus.RUNNING, Instant.now());
        jobRepository.saveAndFlush(runtime.entity());
        launchNow = true;
      }
    }
    if (launchNow) {
      launchWorkerForSlotOwner(runtime, true);
    }
    return new TrainingControlResponse(runtime.jobId(), runtime.status(), "Training reset.", null, true);
  }

  public TrainingControlResponse save(String jobId) {
    return save(requireRuntime(jobId));
  }

  public TrainingControlResponse save(String username, String jobId) {
    return save(requireOwnedRuntime(username, jobId));
  }

  private TrainingControlResponse save(ManagedJobRuntime runtime) {
    if (runtime.owner() == null || runtime.owner().isBlank()) {
      throw new IllegalArgumentException("Please login before saving checkpoints.");
    }
    if (runtime.testResult() == null) {
      throw new IllegalArgumentException("Checkpoint can be saved after test set evaluation completes.");
    }
    TrainingCheckpoint existing = checkpoints
        .findFirstByJobIdAndUserUsernameOrderByCreatedAtDesc(runtime.jobId(), runtime.owner())
        .orElse(null);
    TrainingCheckpoint checkpoint = existing != null ? existing : saveCheckpoint(runtime, runtime.testResult());
    runtime.setCheckpointSaved(true);
    return new TrainingControlResponse(
        runtime.jobId(), runtime.status(), "Checkpoint saved: " + checkpoint.getName(), runtime.pendingControl(), true
    );
  }

  public List<TrainingCheckpointSummary> listCheckpoints(String username, String datasetId) {
    requireUser(username);
    List<TrainingCheckpoint> rows = datasetId == null || datasetId.isBlank()
        ? checkpoints.findByUserUsernameOrderByCreatedAtDesc(username)
        : checkpoints.findByUserUsernameAndDatasetIdOrderByCreatedAtDesc(username, datasetId);
    return rows.stream()
        .map(this::toCheckpointSummary)
        .toList();
  }

  public CheckpointTestResult testCheckpoint(String username, Long checkpointId, TestCheckpointRequest request) {
    requireUser(username);
    TrainingCheckpoint checkpoint = requireCheckpoint(username, checkpointId);
    return runCheckpointTest(checkpoint);
  }

  public InferenceSampleListResponse listCheckpointSamples(String username, Long checkpointId, int limit) {
    requireUser(username);
    TrainingCheckpoint checkpoint = requireCheckpoint(username, checkpointId);
    requireCompletedCheckpoint(checkpoint);
    try {
      JsonNode result = runCheckpointWorker(
          checkpoint,
          "list_checkpoint_samples",
          Map.of("limit", Math.max(1, Math.min(120, limit))),
          "sample_list",
          5
      );
      return objectMapper.treeToValue(result, InferenceSampleListResponse.class);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException("Failed to parse checkpoint samples: " + ex.getMessage());
    }
  }

  public SingleInferenceResult inferCheckpointSample(String username, Long checkpointId, SingleInferenceRequest request) {
    requireUser(username);
    TrainingCheckpoint checkpoint = requireCheckpoint(username, checkpointId);
    requireCompletedCheckpoint(checkpoint);
    try {
      JsonNode result = runCheckpointWorker(
          checkpoint,
          "infer_checkpoint_sample",
          Map.of("sampleIndex", request.sampleIndex()),
          "single_inference",
          5
      );
      return objectMapper.treeToValue(result, SingleInferenceResult.class);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException("Failed to parse inference result: " + ex.getMessage());
    }
  }

  public void addSession(String username, String jobId, WebSocketSession session) {
    ManagedJobRuntime runtime = requireOwnedRuntime(username, jobId);
    addSession(runtime, session);
  }

  public void addCollaborationObserverSession(String jobId, WebSocketSession session) {
    addSession(requireRuntime(jobId), session);
  }

  private void addSession(ManagedJobRuntime runtime, WebSocketSession session) {
    sessions.computeIfAbsent(runtime.jobId(), ignored -> new CopyOnWriteArraySet<>()).add(session);
    List<String> recentEvents = runtime.recentStreamEvents();
    if (!recentEvents.isEmpty()) {
      for (String event : recentEvents) {
        sendRaw(session, event);
      }
      return;
    }
    TrainingMetricMessage latest = runtime.latestMetric();
    if (latest != null) {
      send(session, latest);
    }
  }

  public void removeSession(WebSocketSession session) {
    sessions.values().forEach(set -> set.remove(session));
  }

  public int activeSlotCount() {
    synchronized (schedulerLock) {
      return activeSlotJobIds.size();
    }
  }

  public int queuedJobCount() {
    synchronized (schedulerLock) {
      return waitQueue.size();
    }
  }

  public int activeManagedProcessCount() {
    int count = 0;
    for (ManagedJobRuntime runtime : runtimes.values()) {
      ManagedWorkerProcess proc = runtime.process();
      if (proc != null && proc.isAlive()) {
        count++;
      }
    }
    return count;
  }

  @PreDestroy
  void shutdown() {
    shuttingDown = true;
    runtimes.values().forEach(ManagedJobRuntime::destroyProcessForcibly);
    timerExecutor.shutdownNow();
    workerIoExecutor.shutdownNow();
  }

  private void launchWorkerForSlotOwner(ManagedJobRuntime runtime, boolean rethrowOnFailure) {
    try {
      Files.createDirectories(jobRoot);
      Files.createDirectories(runtime.directory());
      runtime.writeRequest();
      writeControl(runtime, "running");
      ManagedWorkerProcess process = processLauncher.launch(new WorkerLaunchRequest(
          runtime.jobId(),
          runtime.directory(),
          runtime.requestFile(),
          runtime.controlFile(),
          runtime.checkpointFile()
      ));
      runtime.setProcess(process);
      scheduleJobTimeout(runtime, process);
      workerIoExecutor.submit(() -> readWorkerOutput(runtime, process));
      workerIoExecutor.submit(() -> waitForWorkerExit(runtime, process));
    } catch (IOException ex) {
      List<ManagedJobRuntime> promoted = new ArrayList<>();
      synchronized (schedulerLock) {
        if (!runtime.statusEnum().isTerminal()) {
          transitionJobLocked(
              runtime,
              TrainingJobStatus.FAILED,
              null,
              "WORKER_START_FAILED",
              "Failed to start Python training worker: " + ex.getMessage()
          );
        }
        releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
      }
      launchPromotedRuntimes(promoted);
      if (rethrowOnFailure) {
        throw new TrainingApiException(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "WORKER_START_FAILED",
            "Failed to start Python training worker: " + ex.getMessage(),
            Map.of("jobId", runtime.jobId())
        );
      }
    }
  }

  private void scheduleJobTimeout(ManagedJobRuntime runtime, ManagedWorkerProcess process) {
    Duration timeout = properties.getJobTimeout();
    if (timeout == null || timeout.isZero() || timeout.isNegative()) {
      return;
    }
    ScheduledFuture<?> future = timerExecutor.schedule(() -> onJobTimeout(runtime, process, timeout), timeout.toMillis(), TimeUnit.MILLISECONDS);
    runtime.setJobTimeoutFuture(future);
  }

  private void onJobTimeout(ManagedJobRuntime runtime, ManagedWorkerProcess process, Duration timeout) {
    if (shuttingDown || !runtime.isCurrentProcess(process)) {
      return;
    }
    List<ManagedJobRuntime> promoted = new ArrayList<>();
    String reason = "Training job exceeded maximum allowed duration of " + timeout + ".";
    synchronized (schedulerLock) {
      if (runtime.statusEnum().isTerminal() || !runtime.isCurrentProcess(process)) {
        return;
      }
      runtime.destroyProcessForcibly();
      transitionJobLocked(runtime, TrainingJobStatus.FAILED, null, "JOB_TIMEOUT", reason);
      releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
    }
    emitSyntheticError(runtime, "JOB_TIMEOUT", reason);
    launchPromotedRuntimes(promoted);
  }

  private void readWorkerOutput(ManagedJobRuntime runtime, ManagedWorkerProcess process) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.outputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (shuttingDown || !runtime.isCurrentProcess(process)) {
          return;
        }
        handleWorkerLine(runtime, line.trim());
      }
    } catch (IOException ignored) {
      // Process exit handler reconciles final state and slot cleanup.
    }
  }

  private void handleWorkerLine(ManagedJobRuntime runtime, String line) {
    if (line.isBlank() || !line.startsWith("{")) {
      return;
    }
    JsonNode node;
    try {
      node = objectMapper.readTree(line);
    } catch (JsonProcessingException ex) {
      return;
    }
    String type = node.path("type").asText("");
    String reportedDevice = node.path("device").asText(null);
    if (reportedDevice != null && !reportedDevice.isBlank()) {
      runtime.updateDevice(reportedDevice.trim());
    }

    if ("metric".equals(type)) {
      try {
        TrainingMetricMessage metric = objectMapper.treeToValue(node, TrainingMetricMessage.class);
        runtime.updateMetrics(metric);
        synchronized (schedulerLock) {
          jobRepository.saveAndFlush(runtime.entity());
        }
        runtime.addStreamEvent(line);
        broadcastRaw(runtime.jobId(), line);
      } catch (JsonProcessingException ignored) {
        // Ignore malformed metric rows from the worker.
      }
    } else if ("control".equals(type)) {
      String workerStatus = node.path("status").asText("");
      handleWorkerControlEvent(runtime, workerStatus);
      runtime.addStreamEvent(line);
      broadcastRaw(runtime.jobId(), line);
    } else if ("test_result".equals(type)) {
      runtime.setTestResult(node);
      runtime.addStreamEvent(line);
      if (!runtime.checkpointSaved() && runtime.owner() != null && !runtime.owner().isBlank()) {
        try {
          saveCheckpoint(runtime, node);
          runtime.setCheckpointSaved(true);
        } catch (RuntimeException ignored) {
          // Keep streaming the test result even if persistence fails.
        }
      }
      broadcastRaw(runtime.jobId(), line);
    } else if ("backprop".equals(type)) {
      runtime.addStreamEvent(line);
      broadcastRaw(runtime.jobId(), line);
    } else if ("error".equals(type)) {
      String errorCode = node.path("errorCode").asText("WORKER_RUNTIME_ERROR");
      String message = node.path("message").asText("Python training worker failed.");
      runtime.recordWorkerError(errorCode, message);
      runtime.addStreamEvent(line);
      broadcastRaw(runtime.jobId(), line);
    }
  }

  private void handleWorkerControlEvent(ManagedJobRuntime runtime, String workerStatus) {
    if (workerStatus == null || workerStatus.isBlank()) {
      return;
    }
    List<ManagedJobRuntime> promoted = new ArrayList<>();
    synchronized (schedulerLock) {
      TrainingJobStatus current = runtime.statusEnum();
      if (current.isTerminal()) {
        return;
      }
      switch (workerStatus) {
        case "paused" -> {
          if (TrainingJobStateMachine.canTransition(current, TrainingJobStatus.PAUSED)) {
            transitionJobLocked(runtime, TrainingJobStatus.PAUSED, null, null, null);
          }
        }
        case "running" -> {
          if (TrainingJobStateMachine.canTransition(current, TrainingJobStatus.RUNNING)) {
            transitionJobLocked(runtime, TrainingJobStatus.RUNNING, null, null, null);
          }
        }
        case "completed" -> {
          if (TrainingJobStateMachine.canTransition(current, TrainingJobStatus.COMPLETED)) {
            transitionJobLocked(runtime, TrainingJobStatus.COMPLETED, 0, null, null);
            releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
          }
        }
        case "stopped" -> {
          if (TrainingJobStateMachine.canTransition(current, TrainingJobStatus.STOPPED)) {
            transitionJobLocked(runtime, TrainingJobStatus.STOPPED, 0, null, null);
            releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
          }
        }
        default -> {
          // Ignore unknown worker status values.
        }
      }
    }
    launchPromotedRuntimes(promoted);
  }

  private void waitForWorkerExit(ManagedJobRuntime runtime, ManagedWorkerProcess process) {
    try {
      int exitCode = process.waitFor();
      if (shuttingDown) {
        return;
      }
      List<ManagedJobRuntime> promoted = new ArrayList<>();
      synchronized (schedulerLock) {
        if (!runtime.isCurrentProcessOrTerminated(process)) {
          return;
        }
        runtime.clearProcessReference(process);
        TrainingJobStatus current = runtime.statusEnum();
        if (current.isTerminal()) {
          if (runtime.entity().getExitCode() == null) {
            runtime.entity().setExitCode(exitCode);
            runtime.entity().setUpdatedAt(Instant.now());
            jobRepository.saveAndFlush(runtime.entity());
          }
          releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
        } else if (exitCode == 0 && current == TrainingJobStatus.RUNNING) {
          transitionJobLocked(runtime, TrainingJobStatus.COMPLETED, 0, null, null);
          releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
        } else {
          String errCode = runtime.lastWorkerErrorCode() != null ? runtime.lastWorkerErrorCode() : "WORKER_NON_ZERO_EXIT";
          String errReason = runtime.lastWorkerErrorMessage() != null
              ? runtime.lastWorkerErrorMessage()
              : "Training worker exited abnormally with code " + exitCode + ".";
          transitionJobLocked(runtime, TrainingJobStatus.FAILED, exitCode, errCode, errReason);
          releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
        }
      }
      launchPromotedRuntimes(promoted);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  private void transitionJobLocked(
      ManagedJobRuntime runtime,
      TrainingJobStatus targetStatus,
      Integer exitCode,
      String errorCode,
      String errorReason
  ) {
    TrainingJobStatus current = runtime.statusEnum();
    if (current != targetStatus) {
      TrainingJobStateMachine.requireTransition(runtime.jobId(), current, targetStatus);
    }
    Instant now = Instant.now();
    TrainingJob entity = runtime.entity();
    entity.setStatus(targetStatus.wireValue());
    entity.setPendingControl(null);
    entity.setUpdatedAt(now);
    if (exitCode != null) {
      entity.setExitCode(exitCode);
    }
    if (errorCode != null) {
      entity.setErrorCode(errorCode);
    }
    if (errorReason != null) {
      entity.setErrorReason(errorReason);
    }
    if (targetStatus.isTerminal()) {
      entity.setFinishedAt(now);
      runtime.cancelTimers();
    }
    jobRepository.saveAndFlush(entity);
  }

  private void releaseSlotAndCollectNextLocked(String finishedJobId, List<ManagedJobRuntime> promotedOut) {
    activeSlotJobIds.remove(finishedJobId);
    while (activeSlotJobIds.size() < properties.getMaxConcurrentJobs() && !waitQueue.isEmpty()) {
      String nextJobId = waitQueue.pollFirst();
      if (nextJobId == null) {
        break;
      }
      ManagedJobRuntime candidate = runtimes.get(nextJobId);
      if (candidate == null || candidate.statusEnum() != TrainingJobStatus.QUEUED) {
        continue;
      }
      Instant now = Instant.now();
      TrainingJobStateMachine.requireTransition(nextJobId, TrainingJobStatus.QUEUED, TrainingJobStatus.RUNNING);
      candidate.entity().setStatus(TrainingJobStatus.RUNNING.wireValue());
      candidate.entity().setStartedAt(now);
      candidate.entity().setUpdatedAt(now);
      jobRepository.saveAndFlush(candidate.entity());
      activeSlotJobIds.add(nextJobId);
      promotedOut.add(candidate);
    }
  }

  private void launchPromotedRuntimes(List<ManagedJobRuntime> promoted) {
    for (ManagedJobRuntime next : promoted) {
      launchWorkerForSlotOwner(next, false);
    }
  }

  private void emitSyntheticError(ManagedJobRuntime runtime, String errorCode, String message) {
    ObjectNode node = objectMapper.createObjectNode();
    node.put("type", "error");
    node.put("jobId", runtime.jobId());
    node.put("errorCode", errorCode);
    node.put("message", message);
    String payload = node.toString();
    runtime.addStreamEvent(payload);
    broadcastRaw(runtime.jobId(), payload);
  }

  private void broadcastRaw(String jobId, String payload) {
    CopyOnWriteArraySet<WebSocketSession> jobSessions = sessions.get(jobId);
    if (jobSessions == null || jobSessions.isEmpty()) {
      return;
    }
    for (WebSocketSession session : jobSessions) {
      if (!session.isOpen()) {
        jobSessions.remove(session);
        continue;
      }
      try {
        session.sendMessage(new TextMessage(payload));
      } catch (IOException ex) {
        jobSessions.remove(session);
      }
    }
  }

  private void send(WebSocketSession session, TrainingMetricMessage metric) {
    if (!session.isOpen()) {
      return;
    }
    try {
      session.sendMessage(new TextMessage(objectMapper.writeValueAsString(metric)));
    } catch (IOException ex) {
      removeSession(session);
    }
  }

  private void sendRaw(WebSocketSession session, String payload) {
    if (!session.isOpen()) {
      return;
    }
    try {
      session.sendMessage(new TextMessage(payload));
    } catch (IOException ex) {
      removeSession(session);
    }
  }

  private void writeControl(ManagedJobRuntime runtime, String command) {
    try {
      Files.createDirectories(runtime.directory());
      Files.writeString(
          runtime.controlFile(),
          objectMapper.writeValueAsString(Map.of("command", command)),
          StandardCharsets.UTF_8
      );
    } catch (IOException ex) {
      throw new IllegalArgumentException("Failed to update training control file.");
    }
  }

  private ManagedJobRuntime requireRuntime(String jobId) {
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime == null) {
      throw new IllegalArgumentException("Training job not found.");
    }
    return runtime;
  }

  private void requireUser(String username) {
    if (username == null || username.isBlank()) {
      throw new IllegalArgumentException("Please login first.");
    }
  }

  private ManagedJobRuntime requireOwnedRuntime(String username, String jobId) {
    requireUser(username);
    ManagedJobRuntime runtime = requireRuntime(jobId);
    if (runtime.owner() == null || !runtime.owner().equals(username)) {
      throw new IllegalArgumentException("Training job not found.");
    }
    return runtime;
  }

  private TrainingCheckpoint saveCheckpoint(ManagedJobRuntime runtime, JsonNode testResult) {
    AppUser user = users.findByUsername(runtime.owner())
        .orElseThrow(() -> new IllegalArgumentException("User not found."));
    if (!Files.exists(runtime.checkpointFile())) {
      throw new IllegalArgumentException("Checkpoint file is not available yet.");
    }
    TrainingDatasetDetail dataset = datasetService.getDetail(runtime.request().datasetId(), runtime.owner());
    try {
      String layersJson = objectMapper.writeValueAsString(
          runtime.request().layers() == null ? List.of() : runtime.request().layers()
      );
      String configJson = objectMapper.writeValueAsString(runtime.request().config());
      String splitJson = objectMapper.writeValueAsString(runtime.request().split());
      String testResultJson = objectMapper.writeValueAsString(testResult);
      String metricHistoryJson = objectMapper.writeValueAsString(runtime.metricHistory());
      TrainingMetricMessage metric = runtime.latestMetric();
      String checkpointStatus = runtime.epoch() >= runtime.totalEpochs()
          && runtime.statusEnum() != TrainingJobStatus.STOPPED
          && runtime.statusEnum() != TrainingJobStatus.FAILED
          ? TrainingJobStatus.COMPLETED.wireValue()
          : runtime.status();
      TrainingCheckpoint checkpoint = new TrainingCheckpoint(
          user,
          dataset.name() + " · " + runtime.jobId(),
          runtime.jobId(),
          runtime.request().datasetId(),
          dataset.name(),
          runtime.modelSignature(),
          runtime.checkpointFile().toString(),
          layersJson,
          configJson,
          splitJson,
          testResultJson,
          metricHistoryJson,
          describeNetwork(runtime.request().layers()),
          checkpointStatus,
          runtime.epoch(),
          runtime.totalEpochs(),
          metric == null ? null : metric.loss(),
          metric == null ? null : metric.accuracy(),
          metric == null ? null : metric.valLoss(),
          metric == null ? null : metric.valAccuracy(),
          testResult.path("testLoss").isNull() || testResult.path("testLoss").isMissingNode()
              ? null
              : testResult.path("testLoss").asDouble(),
          testResult.path("testAccuracy").isNull() || testResult.path("testAccuracy").isMissingNode()
              ? null
              : testResult.path("testAccuracy").asDouble(),
          testResult.path("sampleCount").asInt(0)
      );
      return checkpoints.save(checkpoint);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException("Failed to serialize checkpoint metadata.");
    }
  }

  private TrainingCheckpointSummary toCheckpointSummary(TrainingCheckpoint checkpoint) {
    JsonNode layers = readCheckpointJson(checkpoint.getLayersJson(), objectMapper.createArrayNode());
    JsonNode config = readCheckpointJson(checkpoint.getConfigJson(), objectMapper.createObjectNode());
    JsonNode split = readCheckpointJson(checkpoint.getSplitJson(), objectMapper.createObjectNode());
    JsonNode testResult = readCheckpointJson(checkpoint.getTestResultJson(), objectMapper.createObjectNode());
    JsonNode metricHistory = readCheckpointJson(checkpoint.getMetricHistoryJson(), objectMapper.createArrayNode());
    List<String> layerSummary = summarizeLayers(layers);
    String description = checkpoint.getNetworkDescription();
    if (description == null || description.isBlank()) {
      description = String.join(" -> ", layerSummary);
    }
    return new TrainingCheckpointSummary(
        checkpoint.getId(),
        checkpoint.getName(),
        checkpoint.getJobId(),
        checkpoint.getDatasetId(),
        checkpoint.getDatasetName(),
        checkpoint.getModelSignature(),
        description,
        layerSummary,
        layers,
        config,
        split,
        testResult,
        metricHistory,
        checkpoint.getStatus(),
        checkpoint.getEpoch(),
        checkpoint.getTotalEpochs(),
        checkpoint.getTrainLoss(),
        checkpoint.getTrainAccuracy(),
        checkpoint.getValLoss(),
        checkpoint.getValAccuracy(),
        checkpoint.getTestLoss(),
        checkpoint.getTestAccuracy(),
        checkpoint.getTestSampleCount(),
        checkpoint.getCreatedAt()
    );
  }

  private JsonNode readCheckpointJson(String raw, JsonNode fallback) {
    if (raw == null || raw.isBlank()) {
      return fallback;
    }
    try {
      return objectMapper.readTree(raw);
    } catch (JsonProcessingException ex) {
      return fallback;
    }
  }

  private String describeNetwork(List<JsonNode> layers) {
    List<String> parts = summarizeLayers(
        layers == null ? objectMapper.createArrayNode() : objectMapper.valueToTree(layers)
    );
    return String.join(" -> ", parts);
  }

  private List<String> summarizeLayers(JsonNode layers) {
    List<String> summary = new ArrayList<>();
    if (layers == null || !layers.isArray()) {
      return summary;
    }
    for (JsonNode layer : layers) {
      if (layer.path("enabled").isBoolean() && !layer.path("enabled").asBoolean()) {
        continue;
      }
      summary.add(summarizeLayer(layer));
    }
    return summary;
  }

  private String summarizeLayer(JsonNode layer) {
    String type = layer.path("type").asText("layer");
    JsonNode params = layer.path("params");
    return switch (type) {
      case "input" -> {
        String inputKind = params.path("inputKind").asText("image");
        if ("table".equals(inputKind)) {
          yield "CSV输入(" + params.path("featureCount").asInt(0) + "维)";
        }
        yield "图像输入(" + params.path("width").asInt(0) + "x" + params.path("height").asInt(0) + "x" + params.path("channels").asInt(0) + ")";
      }
      case "conv2d" -> "卷积(" + params.path("outChannels").asInt(0) + "通道,k" + params.path("kernelSize").asInt(0) + ",s" + params.path("stride").asInt(1) + ")";
      case "residual" -> "残差块(" + params.path("outChannels").asInt(0) + "通道,k" + params.path("kernelSize").asInt(0) + ",投影" + (params.path("useProjection").asBoolean(false) ? "开" : "关") + ")";
      case "pool2d" -> "池化(" + params.path("mode").asText("max") + ",k" + params.path("kernelSize").asInt(0) + ")";
      case "flatten" -> "展平";
      case "dense" -> "全连接(" + params.path("units").asInt(0) + "," + params.path("activation").asText("none") + ")";
      case "activation" -> "激活(" + params.path("activationType").asText("none") + ")";
      case "dropout" -> "Dropout(" + String.format(java.util.Locale.US, "%.0f%%", params.path("rate").asDouble(0) * 100) + ")";
      case "output" -> "输出(" + params.path("units").asInt(0) + "类," + params.path("activation").asText("softmax") + ")";
      default -> layer.path("name").asText(type);
    };
  }

  private CheckpointTestResult runCheckpointTest(TrainingCheckpoint checkpoint) {
    try {
      JsonNode result = runCheckpointWorker(checkpoint, "test_checkpoint", Map.of(), "test_result", 10);
      return objectMapper.treeToValue(result, CheckpointTestResult.class);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException("Failed to parse checkpoint test result: " + ex.getMessage());
    }
  }

  private JsonNode runCheckpointWorker(
      TrainingCheckpoint checkpoint,
      String action,
      Map<String, Object> extraPayload,
      String resultType,
      int timeoutMinutes
  ) {
    String testId = action + "-" + checkpoint.getId() + "-" + UUID.randomUUID();
    Path testDir = jobRoot.resolve("checkpoint-tests").resolve(testId).normalize();
    Path requestFile = testDir.resolve("request.json");
    try {
      Files.createDirectories(testDir);
      ObjectNode payload = objectMapper.createObjectNode();
      payload.put("action", action);
      payload.put("jobId", testId);
      payload.put("datasetRoot", datasetRoot.toString());
      payload.put("datasetId", checkpoint.getDatasetId());
      payload.set("split", readCheckpointJson(checkpoint.getSplitJson(), objectMapper.createObjectNode()));
      payload.set("layers", readCheckpointJson(checkpoint.getLayersJson(), objectMapper.createArrayNode()));
      payload.put("checkpointFile", checkpoint.getCheckpointPath());
      ObjectNode extras = objectMapper.valueToTree(extraPayload);
      extras.fields().forEachRemaining(entry -> payload.set(entry.getKey(), entry.getValue()));

      Files.writeString(requestFile, objectMapper.writeValueAsString(payload), StandardCharsets.UTF_8);
      Process process = new ProcessBuilder(pythonExecutable, workerScript.toString(), "--request", requestFile.toString())
          .redirectErrorStream(true)
          .directory(jobRoot.toFile())
          .start();
      JsonNode result = null;
      StringBuilder diagnostics = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.isBlank()) {
            continue;
          }
          if (!line.startsWith("{")) {
            if (diagnostics.length() < 2000) {
              diagnostics.append(line).append(System.lineSeparator());
            }
            continue;
          }
          JsonNode node = objectMapper.readTree(line);
          String type = node.path("type").asText("");
          if ("error".equals(type)) {
            throw new IllegalArgumentException(node.path("message").asText("Checkpoint worker failed."));
          }
          if (resultType.equals(type)) {
            result = node;
          }
        }
      }
      if (!process.waitFor(timeoutMinutes, TimeUnit.MINUTES)) {
        process.destroyForcibly();
        throw new IllegalArgumentException("Checkpoint worker timed out.");
      }
      if (process.exitValue() != 0) {
        String detail = diagnostics.isEmpty() ? "" : " " + diagnostics.toString().trim();
        throw new IllegalArgumentException("Checkpoint worker process failed." + detail);
      }
      if (result == null) {
        String detail = diagnostics.isEmpty() ? "" : " " + diagnostics.toString().trim();
        throw new IllegalArgumentException("Checkpoint worker returned no result." + detail);
      }
      return result;
    } catch (IOException ex) {
      throw new IllegalArgumentException("Failed to run checkpoint worker: " + ex.getMessage());
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalArgumentException("Checkpoint worker interrupted.");
    }
  }

  private TrainingCheckpoint requireCheckpoint(String username, Long checkpointId) {
    return checkpoints.findByIdAndUserUsername(checkpointId, username)
        .orElseThrow(() -> new IllegalArgumentException("Checkpoint not found."));
  }

  private void requireCompletedCheckpoint(TrainingCheckpoint checkpoint) {
    if ("stopped".equals(checkpoint.getStatus()) || checkpoint.getEpoch() < checkpoint.getTotalEpochs()) {
      throw new IllegalArgumentException("Only completed checkpoints can be used for single-sample inference.");
    }
  }

  private String serializeRequest(StartTrainingRequest request) {
    try {
      return objectMapper.writeValueAsString(request);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException("Failed to serialize training request.");
    }
  }

  private String computeConfigDigest(StartTrainingRequest request) {
    ObjectNode root = objectMapper.createObjectNode();
    root.put("datasetId", request.datasetId());
    ObjectNode splitNode = root.putObject("split");
    splitNode.put("train", request.split().train());
    splitNode.put("val", request.split().val());
    splitNode.put("test", request.split().test());
    ArrayNode layersNode = root.putArray("layers");
    for (JsonNode layer : request.layers() == null ? List.<JsonNode>of() : request.layers()) {
      layersNode.add(layer);
    }
    ArrayNode connNode = root.putArray("connections");
    for (JsonNode conn : request.connections() == null ? List.<JsonNode>of() : request.connections()) {
      connNode.add(conn);
    }
    ObjectNode cfg = root.putObject("config");
    TrainingConfigRequest c = request.config();
    cfg.put("batchSize", valueOrDefault(c.batchSize(), 32));
    cfg.put("totalEpochs", valueOrDefault(c.totalEpochs(), 20));
    cfg.put("learningRate", c.learningRate() == null ? 0.001 : c.learningRate());
    cfg.put("optimizer", c.optimizer() == null ? "Adam" : c.optimizer());
    cfg.put("scheduler", c.scheduler() == null ? "none" : c.scheduler());
    cfg.put("lrDecay", c.lrDecay() == null ? 0.9 : c.lrDecay());
    cfg.put("lossFunction", c.lossFunction() == null ? "cross_entropy" : c.lossFunction());
    return sha256Hex(root);
  }

  private String modelSignature(StartTrainingRequest request) {
    return modelSignature(request.datasetId(), request.layers());
  }

  private String modelSignature(String datasetId, List<JsonNode> layers) {
    ObjectNode root = objectMapper.createObjectNode();
    root.put("datasetId", datasetId);
    ArrayNode normalizedLayers = root.putArray("layers");
    for (JsonNode layer : layers == null ? List.<JsonNode>of() : layers) {
      ObjectNode normalized = objectMapper.createObjectNode();
      normalized.set("type", layer.path("type"));
      normalized.set("enabled", layer.has("enabled") ? layer.path("enabled") : objectMapper.getNodeFactory().booleanNode(true));
      normalized.set("params", layer.path("params"));
      normalizedLayers.add(normalized);
    }
    return sha256Hex(root);
  }

  private String sha256Hex(JsonNode node) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(objectMapper.writeValueAsBytes(node));
      return HexFormat.of().formatHex(hash);
    } catch (NoSuchAlgorithmException | JsonProcessingException ex) {
      throw new IllegalArgumentException("Failed to compute configuration digest.");
    }
  }

  private void validateSplit(SplitRequest split) {
    if (split.train() <= 0) {
      throw new IllegalArgumentException("split.train must be greater than 0.");
    }
    if (split.val() < 0 || split.test() < 0) {
      throw new IllegalArgumentException("split ratios cannot be negative.");
    }
    double sum = split.train() + split.val() + split.test();
    if (Math.abs(sum - 1.0) > 0.0001) {
      throw new IllegalArgumentException("split.train + split.val + split.test must equal 1.0.");
    }
  }

  private int valueOrDefault(Integer value, int fallback) {
    return value == null || value <= 0 ? fallback : value;
  }

  private String nextJobId() {
    return "train-" + LocalDateTime.now().format(JOB_ID_TIME) + "-" + UUID.randomUUID().toString().substring(0, 8);
  }

  private String trimTrailingSlash(String value) {
    if (value == null || value.isBlank()) {
      return "";
    }
    return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
  }

  private String streamUrl(String jobId) {
    String path = "/api/training/stream?jobId=" + jobId;
    return streamBaseUrl.isBlank() ? path : streamBaseUrl + path;
  }

  private TrainingStatusResponse toStatusResponse(TrainingJob entity) {
    long elapsed = entity.getElapsedSeconds();
    if (elapsed == 0L && entity.getStartedAt() != null && entity.getFinishedAt() == null) {
      elapsed = Math.max(0L, Duration.between(entity.getStartedAt(), Instant.now()).toSeconds());
    }
    return new TrainingStatusResponse(
        entity.getJobId(),
        entity.getStatus(),
        entity.getEpoch(),
        entity.getBatch(),
        entity.getTotalEpochs(),
        entity.getTotalBatches(),
        entity.getLatestLoss(),
        entity.getLatestValLoss(),
        entity.getLatestAccuracy(),
        entity.getLatestValAccuracy(),
        elapsed,
        entity.getEtaSeconds(),
        entity.getPendingControl(),
        entity.getDevice(),
        entity.getLastSequence(),
        entity.getExitCode(),
        entity.getErrorCode(),
        entity.getErrorReason(),
        entity.getConfigDigest(),
        entity.getCreatedAt(),
        entity.getStartedAt(),
        entity.getUpdatedAt(),
        entity.getFinishedAt()
    );
  }

  static String jobIdFromSession(WebSocketSession session) {
    return queryParamFromSession(session, "jobId");
  }

  static String queryParamFromSession(WebSocketSession session, String name) {
    URI uri = session.getUri();
    if (uri == null || uri.getQuery() == null) {
      return null;
    }
    for (String part : uri.getQuery().split("&")) {
      String[] pair = part.split("=", 2);
      if (pair.length == 2 && Objects.equals(pair[0], name)) {
        return java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
      }
    }
    return null;
  }

  private final class ManagedJobRuntime {
    private final TrainingJob entity;
    private final StartTrainingRequest request;
    private final Path directory;
    private final Path requestFile;
    private final Path controlFile;
    private final Path checkpointFile;
    private volatile TrainingMetricMessage latestMetric;
    private volatile JsonNode testResult;
    private volatile boolean checkpointSaved;
    private volatile ManagedWorkerProcess process;
    private volatile ScheduledFuture<?> jobTimeoutFuture;
    private volatile String lastWorkerErrorCode;
    private volatile String lastWorkerErrorMessage;
    private final ArrayDeque<String> recentStreamEvents = new ArrayDeque<>();
    private final ArrayDeque<TrainingMetricMessage> metricHistory = new ArrayDeque<>();

    private ManagedJobRuntime(TrainingJob entity, StartTrainingRequest request) {
      this.entity = entity;
      this.request = request;
      this.directory = jobRoot.resolve(entity.getJobId()).normalize();
      this.requestFile = directory.resolve("request.json");
      this.controlFile = directory.resolve("control.json");
      this.checkpointFile = directory.resolve("checkpoint.pt");
    }

    private void writeRequest() throws IOException {
      Map<String, Object> payload = Map.of(
          "jobId", entity.getJobId(),
          "datasetRoot", datasetRoot.toString(),
          "controlFile", controlFile.toString(),
          "datasetId", request.datasetId(),
          "split", request.split(),
          "layers", request.layers() == null ? List.of() : request.layers(),
          "connections", request.connections() == null ? List.of() : request.connections(),
          "config", request.config(),
          "checkpointFile", checkpointFile.toString(),
          "modelSignature", entity.getModelSignature() == null ? "" : entity.getModelSignature()
      );
      Files.writeString(requestFile, objectMapper.writeValueAsString(payload), StandardCharsets.UTF_8);
    }

    private TrainingStatusResponse toStatusResponse() {
      return TrainingJobService.this.toStatusResponse(entity);
    }

    private void updateMetrics(TrainingMetricMessage metric) {
      this.latestMetric = metric;
      addMetricHistory(metric);
      entity.setEpoch(metric.epoch());
      entity.setBatch(metric.batch());
      entity.setTotalBatches(metric.totalBatches());
      entity.setLatestLoss(metric.loss());
      entity.setLatestValLoss(metric.valLoss());
      entity.setLatestAccuracy(metric.accuracy());
      entity.setLatestValAccuracy(metric.valAccuracy());
      entity.setElapsedSeconds(metric.elapsedSeconds());
      entity.setEtaSeconds(metric.etaSeconds());
      entity.setUpdatedAt(Instant.now());
    }

    private void updateDevice(String device) {
      synchronized (schedulerLock) {
        if (!Objects.equals(entity.getDevice(), device)) {
          entity.setDevice(device);
          entity.setUpdatedAt(Instant.now());
          jobRepository.saveAndFlush(entity);
        }
      }
    }

    private void recordWorkerError(String errorCode, String message) {
      this.lastWorkerErrorCode = errorCode;
      this.lastWorkerErrorMessage = message;
    }

    private void resetForRerun(TrainingJobStatus status, Instant startedAt) {
      cancelTimers();
      latestMetric = null;
      testResult = null;
      checkpointSaved = false;
      lastWorkerErrorCode = null;
      lastWorkerErrorMessage = null;
      synchronized (recentStreamEvents) {
        recentStreamEvents.clear();
      }
      synchronized (metricHistory) {
        metricHistory.clear();
      }
      Instant now = Instant.now();
      entity.setStatus(status.wireValue());
      entity.setPendingControl(null);
      entity.setStartedAt(startedAt);
      entity.setFinishedAt(null);
      entity.setUpdatedAt(now);
      entity.setExitCode(null);
      entity.setErrorCode(null);
      entity.setErrorReason(null);
      entity.setEpoch(0);
      entity.setBatch(0);
      entity.setElapsedSeconds(0L);
      entity.setEtaSeconds(0L);
    }

    private void addStreamEvent(String payload) {
      int maxEvents = properties.getMaxHistoryEvents();
      synchronized (recentStreamEvents) {
        recentStreamEvents.addLast(payload);
        while (recentStreamEvents.size() > maxEvents) {
          recentStreamEvents.removeFirst();
        }
      }
    }

    private List<String> recentStreamEvents() {
      synchronized (recentStreamEvents) {
        return new ArrayList<>(recentStreamEvents);
      }
    }

    private void addMetricHistory(TrainingMetricMessage metric) {
      synchronized (metricHistory) {
        metricHistory.addLast(metric);
        while (metricHistory.size() > MAX_CHECKPOINT_METRIC_HISTORY) {
          metricHistory.removeFirst();
        }
      }
    }

    private List<TrainingMetricMessage> metricHistory() {
      synchronized (metricHistory) {
        return new ArrayList<>(metricHistory);
      }
    }

    private void destroyProcessForcibly() {
      cancelTimers();
      ManagedWorkerProcess current = process;
      process = null;
      if (current != null) {
        current.destroyForcibly();
      }
    }

    private void cancelTimers() {
      ScheduledFuture<?> jf = jobTimeoutFuture;
      jobTimeoutFuture = null;
      if (jf != null) {
        jf.cancel(false);
      }
    }

    private boolean isCurrentProcess(ManagedWorkerProcess candidate) {
      return process == candidate;
    }

    private boolean isCurrentProcessOrTerminated(ManagedWorkerProcess candidate) {
      return process == candidate || process == null;
    }

    private void clearProcessReference(ManagedWorkerProcess candidate) {
      if (process == candidate) {
        process = null;
      }
    }

    private TrainingJob entity() { return entity; }
    private String jobId() { return entity.getJobId(); }
    private String owner() { return entity.getOwner(); }
    private StartTrainingRequest request() { return request; }
    private String modelSignature() { return entity.getModelSignature(); }
    private int totalEpochs() { return entity.getTotalEpochs(); }
    private String status() { return entity.getStatus(); }
    private TrainingJobStatus statusEnum() { return TrainingJobStatus.fromWireValue(entity.getStatus()); }
    private String pendingControl() { return entity.getPendingControl(); }
    private TrainingControlAction pendingControlEnum() {
      return TrainingControlAction.fromNullableWireValue(entity.getPendingControl());
    }
    private int epoch() { return entity.getEpoch(); }
    private TrainingMetricMessage latestMetric() { return latestMetric; }
    private JsonNode testResult() { return testResult; }
    private void setTestResult(JsonNode testResult) { this.testResult = testResult; }
    private boolean checkpointSaved() { return checkpointSaved; }
    private void setCheckpointSaved(boolean checkpointSaved) { this.checkpointSaved = checkpointSaved; }
    private Path directory() { return directory; }
    private Path requestFile() { return requestFile; }
    private Path controlFile() { return controlFile; }
    private Path checkpointFile() { return checkpointFile; }
    private ManagedWorkerProcess process() { return process; }
    private void setProcess(ManagedWorkerProcess process) { this.process = process; }
    private void setJobTimeoutFuture(ScheduledFuture<?> future) { this.jobTimeoutFuture = future; }
    private String lastWorkerErrorCode() { return lastWorkerErrorCode; }
    private String lastWorkerErrorMessage() { return lastWorkerErrorMessage; }
  }
}
