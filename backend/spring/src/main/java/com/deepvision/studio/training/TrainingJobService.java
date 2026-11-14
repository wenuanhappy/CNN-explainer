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
import com.deepvision.studio.training.TrainingDtos.TrainingEventsResponse;
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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@Service
public class TrainingJobService {
  public static final String EVENT_SCHEMA_VERSION = "1.0";
  static final int EVENT_PRUNE_BATCH_SIZE = 16;
  static final int ENTITY_FLUSH_EVENT_INTERVAL = 16;
  private static final DateTimeFormatter JOB_ID_TIME = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
  private static final int MAX_CHECKPOINT_METRIC_HISTORY = 2000;

  private final TrainingDatasetService datasetService;
  private final AppUserRepository users;
  private final TrainingCheckpointRepository checkpoints;
  private final TrainingJobRepository jobRepository;
  private final TrainingJobEventRepository eventRepository;
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
      String datasetRoot
  ) {
    this(
        datasetService,
        users,
        checkpoints,
        jobRepository,
        null,
        networkValidator,
        processLauncher,
        properties,
        objectMapper,
        datasetRoot
    );
  }

  @Autowired
  public TrainingJobService(
      TrainingDatasetService datasetService,
      AppUserRepository users,
      TrainingCheckpointRepository checkpoints,
      TrainingJobRepository jobRepository,
      TrainingJobEventRepository eventRepository,
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
    this.eventRepository = eventRepository;
    this.networkValidator = networkValidator;
    this.processLauncher = processLauncher;
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.streamBaseUrl = trimTrailingSlash(properties.getStreamBaseUrl());
    this.datasetRoot = Path.of(datasetRoot).toAbsolutePath().normalize();
    this.jobRoot = Path.of(properties.getJobsRoot()).toAbsolutePath().normalize();
    this.pythonExecutable = properties.getPythonExecutable();
    this.workerScript = Path.of(properties.getWorkerScript()).toAbsolutePath().normalize();
    recoverOrphanedJobsOnStartup();
  }

  int recoverOrphanedJobsOnStartup() {
    int recovered = 0;
    synchronized (schedulerLock) {
      List<TrainingJob> orphanedCandidates =
          jobRepository.findByStatusInOrderByCreatedAtAsc(TrainingJobStatus.activeWireValues());
      if (orphanedCandidates == null || orphanedCandidates.isEmpty()) {
        return 0;
      }
      Instant now = Instant.now();
      for (TrainingJob job : orphanedCandidates) {
        ManagedJobRuntime runtime = runtimes.get(job.getJobId());
        ManagedWorkerProcess proc = runtime == null ? null : runtime.process();
        if (proc != null && proc.isAlive()) {
          continue;
        }
        TrainingJobStatus prevStatus = TrainingJobStatus.fromWireValue(job.getStatus());
        TrainingJobStateMachine.requireTransition(job.getJobId(), prevStatus, TrainingJobStatus.INTERRUPTED);
        activeSlotJobIds.remove(job.getJobId());
        waitQueue.remove(job.getJobId());
        job.setStatus(TrainingJobStatus.INTERRUPTED.wireValue());
        job.setPendingControl(null);
        job.setErrorCode("SERVICE_RESTART_INTERRUPTED");
        job.setErrorReason(
            "Service restarted while job was '" + prevStatus.wireValue()
                + "' with no managed worker process; checkpoint resume is not supported."
        );
        job.setUpdatedAt(now);
        job.setFinishedAt(now);
        ObjectNode recoveryEvent = objectMapper.createObjectNode();
        recoveryEvent.put("type", "control");
        recoveryEvent.put("status", TrainingJobStatus.INTERRUPTED.wireValue());
        recoveryEvent.put("previousStatus", prevStatus.wireValue());
        recoveryEvent.put("errorCode", "SERVICE_RESTART_INTERRUPTED");
        recoveryEvent.put("message", job.getErrorReason());
        recordEntityEventLocked(job, recoveryEvent, now);
        recovered++;
      }
    }
    return recovered;
  }

  public TrainingStartResponse start(StartTrainingRequest request, String username) {
    return start(request, username, null);
  }

  public TrainingStartResponse start(StartTrainingRequest request, String username, String idempotencyKey) {
    requireUser(username);
    String normalizedKey = normalizeIdempotencyKey(idempotencyKey);
    String configDigest = computeConfigDigest(request);
    if (normalizedKey != null) {
      synchronized (schedulerLock) {
        TrainingStartResponse earlyReplay =
            findIdempotentReplayOrThrowConflictLocked(username, normalizedKey, configDigest);
        if (earlyReplay != null) {
          return earlyReplay;
        }
      }
    }

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
    String requestJson = serializeRequest(request);

    ScheduledLaunch scheduledLaunch = null;
    Integer queuePosition = null;
    PendingEventWrite pendingQueuedEvent = null;
    String initialStatus;
    String jobId;

    synchronized (schedulerLock) {
      if (normalizedKey != null) {
        TrainingStartResponse raceReplay =
            findIdempotentReplayOrThrowConflictLocked(username, normalizedKey, configDigest);
        if (raceReplay != null) {
          return raceReplay;
        }
      }

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
      entity.setIdempotencyKey(normalizedKey);
      jobRepository.saveAndFlush(entity);

      ManagedJobRuntime runtime = new ManagedJobRuntime(entity, request);
      runtimes.put(jobId, runtime);

      if (hasSlot) {
        activeSlotJobIds.add(jobId);
        scheduledLaunch = new ScheduledLaunch(runtime, runtime.allocateRunGenerationLocked());
      } else {
        waitQueue.addLast(jobId);
        queuePosition = waitQueue.size();
        ObjectNode queuedEvent = objectMapper.createObjectNode();
        queuedEvent.put("type", "control");
        queuedEvent.put("status", TrainingJobStatus.QUEUED.wireValue());
        queuedEvent.put("queuePosition", queuePosition);
        queuedEvent.put("message", "Training job queued.");
        pendingQueuedEvent = prepareRuntimeEventLocked(runtime, queuedEvent, now, true);
      }
    }

    if (pendingQueuedEvent != null) {
      persistAndBroadcastEvent(pendingQueuedEvent);
    }

    if (scheduledLaunch != null) {
      launchWorkerForSlotOwner(scheduledLaunch, true);
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

  private TrainingStartResponse findIdempotentReplayOrThrowConflictLocked(
      String username,
      String normalizedKey,
      String configDigest
  ) {
    TrainingJob existing = jobRepository.findByOwnerAndIdempotencyKey(username, normalizedKey).orElse(null);
    if (existing == null) {
      return null;
    }
    if (!Objects.equals(existing.getConfigDigest(), configDigest)) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("jobId", existing.getJobId());
      details.put("idempotencyKey", normalizedKey);
      details.put("existingConfigDigest", existing.getConfigDigest());
      details.put("requestConfigDigest", configDigest);
      throw new TrainingStateConflictException(
          "IDEMPOTENCY_KEY_CONFLICT",
          "Idempotency-Key has already been used for a different training configuration.",
          details
      );
    }
    Integer existingQueuePos = TrainingJobStatus.QUEUED.wireValue().equals(existing.getStatus())
        ? queuePositionLocked(existing.getJobId())
        : null;
    return new TrainingStartResponse(
        existing.getJobId(),
        existing.getStatus(),
        existing.getTotalEpochs(),
        existing.getTotalBatches(),
        streamUrl(existing.getJobId()),
        existing.getConfigDigest(),
        true,
        existingQueuePos
    );
  }

  public TrainingStatusResponse status(String jobId) {
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      synchronized (schedulerLock) {
        return runtime.toStatusResponse();
      }
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
      synchronized (schedulerLock) {
        return runtime.toStatusResponse();
      }
    }
    TrainingJob entity = jobRepository.findByJobIdAndOwner(jobId, username)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    return toStatusResponse(entity);
  }

  public TrainingEventsResponse events(String jobId, long afterSequence, int limit) {
    validateEventQueryParams(afterSequence, limit);
    ManagedJobRuntime runtime = runtimes.get(jobId);
    TrainingJob entity = runtime != null
        ? runtime.entity()
        : jobRepository.findById(jobId)
            .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    return buildEventsResponse(entity, runtime, afterSequence, limit);
  }

  public TrainingEventsResponse events(String username, String jobId, long afterSequence, int limit) {
    requireUser(username);
    validateEventQueryParams(afterSequence, limit);
    ManagedJobRuntime runtime = runtimes.get(jobId);
    TrainingJob entity;
    if (runtime != null) {
      if (!username.equals(runtime.owner())) {
        throw new IllegalArgumentException("Training job not found.");
      }
      entity = runtime.entity();
    } else {
      entity = jobRepository.findByJobIdAndOwner(jobId, username)
          .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    }
    return buildEventsResponse(entity, runtime, afterSequence, limit);
  }

  private void validateEventQueryParams(long afterSequence, int limit) {
    if (afterSequence < 0) {
      throw new IllegalArgumentException("afterSequence must be greater than or equal to 0.");
    }
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be greater than 0.");
    }
  }

  private TrainingEventsResponse buildEventsResponse(
      TrainingJob entity,
      ManagedJobRuntime runtime,
      long afterSequence,
      int limit
  ) {
    int maxHistory = properties.getMaxHistoryEvents();
    int effectiveLimit = Math.min(limit, maxHistory);
    if (runtime != null) {
      runtime.awaitPendingEventWrites();
    }
    String jobId;
    String currentStatus;
    long lastSequence;
    synchronized (schedulerLock) {
      if (runtime != null && runtime.isEntityDirtyLocked()) {
        jobRepository.saveAndFlush(entity);
        runtime.clearEntityDirtyLocked();
      }
      jobId = entity.getJobId();
      currentStatus = entity.getStatus();
      lastSequence = entity.getLastSequence();
    }
    Object queryLock = runtime != null ? runtime.persistenceLock() : new Object();
    synchronized (queryLock) {
      Long earliestAvailable;
      List<JsonNode> payloads;
      if (eventRepository != null) {
        long targetPruneFloor = lastSequence - maxHistory;
        if (targetPruneFloor > 0L && (runtime == null || targetPruneFloor > runtime.lastPrunedUpToSequence())) {
          eventRepository.deleteByJobIdAndSequenceLessThanEqual(jobId, targetPruneFloor);
          if (runtime != null) {
            runtime.setLastPrunedUpToSequence(targetPruneFloor);
          }
        }
        earliestAvailable = eventRepository.findFirstByJobIdOrderBySequenceAsc(jobId)
            .map(TrainingJobEvent::getSequence)
            .orElse(null);
        List<TrainingJobEvent> rows = eventRepository.findByJobIdAndSequenceGreaterThanOrderBySequenceAsc(
            jobId,
            afterSequence,
            PageRequest.of(0, effectiveLimit)
        );
        payloads = new ArrayList<>(rows.size());
        for (TrainingJobEvent row : rows) {
          try {
            payloads.add(objectMapper.readTree(row.getPayloadJson()));
          } catch (JsonProcessingException ignored) {
            // Skip corrupted rows if any.
          }
        }
      } else if (runtime != null) {
        earliestAvailable = runtime.earliestSequence();
        payloads = runtime.eventsAfter(afterSequence, effectiveLimit);
      } else {
        earliestAvailable = null;
        payloads = List.of();
      }
      boolean truncated = lastSequence > afterSequence
          && (earliestAvailable == null || afterSequence + 1L < earliestAvailable);
      return new TrainingEventsResponse(
          jobId,
          currentStatus,
          EVENT_SCHEMA_VERSION,
          lastSequence,
          earliestAvailable,
          truncated,
          payloads
      );
    }
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
    return pause(resolveRuntimeForControl(jobId, TrainingControlAction.PAUSE));
  }

  public TrainingControlResponse pause(String username, String jobId) {
    return pause(resolveOwnedRuntimeForControl(username, jobId, TrainingControlAction.PAUSE));
  }

  private TrainingControlResponse pause(ManagedJobRuntime runtime) {
    String statusSnapshot;
    synchronized (schedulerLock) {
      TrainingJobStateMachine.validateControlRequest(
          runtime.jobId(), runtime.statusEnum(), runtime.pendingControlEnum(), TrainingControlAction.PAUSE
      );
      writeControl(runtime, "paused");
      runtime.entity().setPendingControl(TrainingControlAction.PAUSE.wireValue());
      runtime.entity().setUpdatedAt(Instant.now());
      jobRepository.saveAndFlush(runtime.entity());
      statusSnapshot = runtime.status();
    }
    return new TrainingControlResponse(
        runtime.jobId(),
        statusSnapshot,
        "Pause request accepted; waiting for worker confirmation.",
        TrainingControlAction.PAUSE.wireValue(),
        true
    );
  }

  public TrainingControlResponse resume(String jobId) {
    return resume(resolveRuntimeForControl(jobId, TrainingControlAction.RESUME));
  }

  public TrainingControlResponse resume(String username, String jobId) {
    return resume(resolveOwnedRuntimeForControl(username, jobId, TrainingControlAction.RESUME));
  }

  private TrainingControlResponse resume(ManagedJobRuntime runtime) {
    String statusSnapshot;
    synchronized (schedulerLock) {
      TrainingJobStateMachine.validateControlRequest(
          runtime.jobId(), runtime.statusEnum(), runtime.pendingControlEnum(), TrainingControlAction.RESUME
      );
      boolean needsSlot = !activeSlotJobIds.contains(runtime.jobId());
      if (needsSlot && activeSlotJobIds.size() >= properties.getMaxConcurrentJobs()) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("jobId", runtime.jobId());
        details.put("reason", "resume_slot_unavailable");
        details.put("maxConcurrentJobs", properties.getMaxConcurrentJobs());
        details.put("activeSlots", activeSlotJobIds.size());
        throw new TrainingCapacityException(
            "TRAINING_CAPACITY_EXCEEDED",
            "No execution slot available to resume paused training job; retry after active jobs finish.",
            details
        );
      }
      writeControl(runtime, "running");
      if (needsSlot) {
        activeSlotJobIds.add(runtime.jobId());
      }
      runtime.entity().setPendingControl(TrainingControlAction.RESUME.wireValue());
      runtime.entity().setUpdatedAt(Instant.now());
      jobRepository.saveAndFlush(runtime.entity());
      statusSnapshot = runtime.status();
    }
    return new TrainingControlResponse(
        runtime.jobId(),
        statusSnapshot,
        "Resume request accepted; waiting for worker confirmation.",
        TrainingControlAction.RESUME.wireValue(),
        true
    );
  }

  public TrainingControlResponse stop(String jobId) {
    return stop(resolveRuntimeForControl(jobId, TrainingControlAction.STOP));
  }

  public TrainingControlResponse stop(String username, String jobId) {
    return stop(resolveOwnedRuntimeForControl(username, jobId, TrainingControlAction.STOP));
  }

  private TrainingControlResponse stop(ManagedJobRuntime runtime) {
    String statusSnapshot;
    PendingEventWrite pendingStoppedQueued = null;
    synchronized (schedulerLock) {
      TrainingJobStateMachine.validateControlRequest(
          runtime.jobId(), runtime.statusEnum(), runtime.pendingControlEnum(), TrainingControlAction.STOP
      );
      if (runtime.statusEnum() == TrainingJobStatus.QUEUED) {
        waitQueue.remove(runtime.jobId());
        transitionJobLocked(runtime, TrainingJobStatus.STOPPED, null, null, "Stopped while queued.");
        ObjectNode stoppedEvent = objectMapper.createObjectNode();
        stoppedEvent.put("type", "control");
        stoppedEvent.put("status", TrainingJobStatus.STOPPED.wireValue());
        stoppedEvent.put("message", "Queued training job stopped.");
        pendingStoppedQueued = prepareRuntimeEventLocked(runtime, stoppedEvent, Instant.now(), true);
        statusSnapshot = runtime.status();
      } else {
        if (runtime.pendingControlEnum() != TrainingControlAction.STOP) {
          writeControl(runtime, "stopped");
          runtime.entity().setPendingControl(TrainingControlAction.STOP.wireValue());
          runtime.entity().setUpdatedAt(Instant.now());
          jobRepository.saveAndFlush(runtime.entity());
          runtime.clearEntityDirtyLocked();
          scheduleStopTimeoutLocked(runtime, runtime.currentRunGenerationLocked());
        }
        statusSnapshot = runtime.status();
      }
    }
    if (pendingStoppedQueued != null) {
      persistAndBroadcastEvent(pendingStoppedQueued);
      return new TrainingControlResponse(runtime.jobId(), statusSnapshot, "Queued training job stopped.", null, true);
    }
    return new TrainingControlResponse(
        runtime.jobId(),
        statusSnapshot,
        "Stop request accepted; waiting for worker confirmation.",
        TrainingControlAction.STOP.wireValue(),
        true
    );
  }

  public TrainingControlResponse reset(String jobId) {
    return resetInternal(null, jobId);
  }

  public TrainingControlResponse reset(String username, String jobId) {
    requireUser(username);
    return resetInternal(username, jobId);
  }

  private TrainingControlResponse resetInternal(String username, String jobId) {
    StartTrainingRequest sourceRequest;
    String effectiveOwner;
    synchronized (schedulerLock) {
      ManagedJobRuntime runtime = runtimes.get(jobId);
      TrainingJob entity;
      if (runtime != null) {
        if (username != null && !Objects.equals(username, runtime.owner())) {
          throw new IllegalArgumentException("Training job not found.");
        }
        entity = runtime.entity();
        sourceRequest = runtime.request();
      } else {
        entity = (username == null
            ? jobRepository.findById(jobId)
            : jobRepository.findByJobIdAndOwner(jobId, username))
            .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
        sourceRequest = deserializeRequest(entity.getRequestJson());
      }
      TrainingJobStatus currentStatus = TrainingJobStatus.fromWireValue(entity.getStatus());
      if (!currentStatus.isTerminal()) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("jobId", jobId);
        details.put("currentStatus", currentStatus.wireValue());
        details.put("requestedAction", "reset");
        throw new TrainingStateConflictException(
            "INVALID_STATE_TRANSITION",
            "Cannot reset non-terminal training job " + jobId
                + " in status '" + currentStatus.wireValue() + "'; stop it first.",
            details
        );
      }
      effectiveOwner = username != null ? username : entity.getOwner();
    }
    TrainingStartResponse restarted = start(sourceRequest, effectiveOwner, null);
    return new TrainingControlResponse(
        restarted.jobId(),
        restarted.status(),
        "Training reset as new job " + restarted.jobId() + ".",
        null,
        true
    );
  }

  // Package-private helper for testing runGeneration callback isolation on a superseded runtime.
  void relaunchWithNewGenerationForTest(String jobId) {
    ScheduledLaunch nextLaunch;
    synchronized (schedulerLock) {
      ManagedJobRuntime runtime = requireRuntime(jobId);
      runtime.invalidateRunGenerationAndDestroyProcessLocked();
      activeSlotJobIds.add(jobId);
      Instant now = Instant.now();
      runtime.entity().setStatus(TrainingJobStatus.RUNNING.wireValue());
      runtime.entity().setPendingControl(null);
      runtime.entity().setStartedAt(now);
      runtime.entity().setFinishedAt(null);
      runtime.entity().setUpdatedAt(now);
      runtime.entity().setExitCode(null);
      runtime.entity().setErrorCode(null);
      runtime.entity().setErrorReason(null);
      jobRepository.saveAndFlush(runtime.entity());
      nextLaunch = new ScheduledLaunch(runtime, runtime.allocateRunGenerationLocked());
    }
    launchWorkerForSlotOwner(nextLaunch, true);
  }

  public TrainingControlResponse save(String jobId) {
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      return save(runtime);
    }
    TrainingJob entity = jobRepository.findById(jobId)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    return saveFromPersistedJob(entity);
  }

  public TrainingControlResponse save(String username, String jobId) {
    requireUser(username);
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      if (runtime.owner() == null || !runtime.owner().equals(username)) {
        throw new IllegalArgumentException("Training job not found.");
      }
      return save(runtime);
    }
    TrainingJob entity = jobRepository.findByJobIdAndOwner(jobId, username)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    return saveFromPersistedJob(entity);
  }

  private TrainingControlResponse saveFromPersistedJob(TrainingJob entity) {
    if (entity.getOwner() == null || entity.getOwner().isBlank()) {
      throw new IllegalArgumentException("Please login before saving checkpoints.");
    }
    TrainingCheckpoint existing = checkpoints
        .findFirstByJobIdAndUserUsernameOrderByCreatedAtDesc(entity.getJobId(), entity.getOwner())
        .orElseThrow(() -> new IllegalArgumentException("Checkpoint can be saved after test set evaluation completes."));
    return new TrainingControlResponse(
        entity.getJobId(),
        entity.getStatus(),
        "Checkpoint saved: " + existing.getName(),
        entity.getPendingControl(),
        true
    );
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
    requireUser(username);
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      if (runtime.owner() == null || !runtime.owner().equals(username)) {
        throw new IllegalArgumentException("Training job not found.");
      }
      addSession(runtime, session);
      return;
    }
    TrainingJob entity = jobRepository.findByJobIdAndOwner(jobId, username)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    replayPersistedEventsToSession(entity.getJobId(), session);
  }

  public void addCollaborationObserverSession(String jobId, WebSocketSession session) {
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      addSession(runtime, session);
      return;
    }
    TrainingJob entity = jobRepository.findById(jobId)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    replayPersistedEventsToSession(entity.getJobId(), session);
  }

  private void replayPersistedEventsToSession(String jobId, WebSocketSession session) {
    sessions.computeIfAbsent(jobId, ignored -> new CopyOnWriteArraySet<>()).add(session);
    if (eventRepository == null) {
      return;
    }
    List<TrainingJobEvent> rows = eventRepository.findByJobIdAndSequenceGreaterThanOrderBySequenceAsc(
        jobId, 0L, PageRequest.of(0, properties.getMaxHistoryEvents())
    );
    for (TrainingJobEvent row : rows) {
      sendRaw(session, row.getPayloadJson());
    }
  }

  private void addSession(ManagedJobRuntime runtime, WebSocketSession session) {
    sessions.computeIfAbsent(runtime.jobId(), ignored -> new CopyOnWriteArraySet<>()).add(session);
    List<String> recentEvents = runtime.recentStreamEvents();
    for (String event : recentEvents) {
      sendRaw(session, event);
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

  int runtimeCountForTest() {
    return runtimes.size();
  }

  @PreDestroy
  void shutdown() {
    shuttingDown = true;
    synchronized (schedulerLock) {
      for (ManagedJobRuntime runtime : runtimes.values()) {
        runtime.invalidateRunGenerationAndDestroyProcessLocked();
        if (runtime.isEntityDirtyLocked()) {
          jobRepository.saveAndFlush(runtime.entity());
          runtime.clearEntityDirtyLocked();
        }
      }
    }
    timerExecutor.shutdownNow();
    workerIoExecutor.shutdownNow();
  }

  private void launchWorkerForSlotOwner(ScheduledLaunch launch, boolean rethrowOnFailure) {
    ManagedJobRuntime runtime = launch.runtime();
    long runGeneration = launch.runGeneration();
    try {
      Files.createDirectories(jobRoot);
      Files.createDirectories(runtime.directory());
      runtime.writeRequest();
      synchronized (schedulerLock) {
        if (shuttingDown || !runtime.isActiveRunGeneration(runGeneration) || runtime.statusEnum().isTerminal()) {
          return;
        }
        String initialCommand = runtime.pendingControlEnum() == TrainingControlAction.STOP ? "stopped" : "running";
        writeControl(runtime, initialCommand);
      }
      ManagedWorkerProcess process = processLauncher.launch(new WorkerLaunchRequest(
          runtime.jobId(),
          runtime.directory(),
          runtime.requestFile(),
          runtime.controlFile(),
          runtime.checkpointFile()
      ));
      synchronized (schedulerLock) {
        if (shuttingDown || !runtime.isActiveRunGeneration(runGeneration) || runtime.statusEnum().isTerminal()) {
          process.destroyForcibly();
          return;
        }
        if (runtime.pendingControlEnum() == TrainingControlAction.STOP) {
          writeControl(runtime, "stopped");
        }
        runtime.attachProcessLocked(runGeneration, process);
        scheduleJobTimeoutLocked(runtime, runGeneration);
      }
      Future<?> outputReaderFuture = workerIoExecutor.submit(() -> readWorkerOutput(runtime, process, runGeneration));
      workerIoExecutor.submit(() -> waitForWorkerExit(runtime, process, runGeneration, outputReaderFuture));
    } catch (IOException | RuntimeException ex) {
      String detail = ex.getMessage() == null || ex.getMessage().isBlank()
          ? ex.getClass().getSimpleName()
          : ex.getMessage();
      String startErrorMessage = "Failed to start Python training worker: " + detail;
      List<ScheduledLaunch> promoted = new ArrayList<>();
      boolean shouldEmitError = false;
      synchronized (schedulerLock) {
        if (runtime.isActiveRunGeneration(runGeneration)) {
          runtime.invalidateRunGenerationAndDestroyProcessLocked();
          if (!runtime.statusEnum().isTerminal()) {
            transitionJobLocked(
                runtime,
                TrainingJobStatus.FAILED,
                null,
                "WORKER_START_FAILED",
                startErrorMessage
            );
            shouldEmitError = true;
          }
          releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
        }
      }
      if (shouldEmitError) {
        emitSyntheticError(runtime, "WORKER_START_FAILED", startErrorMessage);
      }
      launchPromotedRuntimes(promoted);
      if (rethrowOnFailure) {
        throw new TrainingApiException(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "WORKER_START_FAILED",
            startErrorMessage,
            Map.of("jobId", runtime.jobId())
        );
      }
    }
  }

  private void scheduleJobTimeoutLocked(ManagedJobRuntime runtime, long runGeneration) {
    Duration timeout = properties.getJobTimeout();
    if (timeout == null || timeout.isZero() || timeout.isNegative()) {
      return;
    }
    runtime.initializeJobTimeoutLocked(timeout.toNanos());
    scheduleRemainingJobTimeoutLocked(runtime, runGeneration, timeout);
  }

  private void resumeJobTimeoutLocked(ManagedJobRuntime runtime, long runGeneration) {
    Duration timeout = properties.getJobTimeout();
    if (timeout == null || timeout.isZero() || timeout.isNegative()) {
      return;
    }
    runtime.markJobTimeoutResumedLocked(timeout.toNanos());
    scheduleRemainingJobTimeoutLocked(runtime, runGeneration, timeout);
  }

  private void scheduleRemainingJobTimeoutLocked(
      ManagedJobRuntime runtime,
      long runGeneration,
      Duration configuredTimeout
  ) {
    long remainingNanos = runtime.remainingJobTimeoutNanosLocked();
    if (remainingNanos <= 0L) {
      remainingNanos = 1L;
    }
    ScheduledFuture<?> future = timerExecutor.schedule(
        () -> onJobTimeout(runtime, runGeneration, configuredTimeout),
        remainingNanos,
        TimeUnit.NANOSECONDS
    );
    runtime.setJobTimeoutFuture(future);
  }

  private void scheduleStopTimeoutLocked(ManagedJobRuntime runtime, long runGeneration) {
    Duration timeout = properties.getStopTimeout();
    if (timeout == null || timeout.isZero() || timeout.isNegative()) {
      timeout = Duration.ofMillis(1);
    }
    final Duration effectiveTimeout = timeout;
    ScheduledFuture<?> future = timerExecutor.schedule(
        () -> onStopTimeout(runtime, runGeneration, effectiveTimeout),
        effectiveTimeout.toMillis(),
        TimeUnit.MILLISECONDS
    );
    runtime.setStopTimeoutFuture(future);
  }

  private void onStopTimeout(ManagedJobRuntime runtime, long runGeneration, Duration timeout) {
    if (shuttingDown || !runtime.isActiveRunGeneration(runGeneration)) {
      return;
    }
    List<ScheduledLaunch> promoted = new ArrayList<>();
    String reason = "Worker did not confirm stop within " + timeout + "; forcibly terminated.";
    synchronized (schedulerLock) {
      if (!runtime.isActiveRunGeneration(runGeneration)
          || runtime.statusEnum().isTerminal()
          || runtime.pendingControlEnum() != TrainingControlAction.STOP) {
        return;
      }
      ManagedWorkerProcess terminatedProc = runtime.invalidateRunGenerationAndDestroyProcessLocked();
      Integer forcedExit = null;
      if (terminatedProc != null && !terminatedProc.isAlive()) {
        try {
          forcedExit = terminatedProc.exitValue();
        } catch (IllegalThreadStateException ignored) {
          // Process still terminating in OS.
        }
      }
      transitionJobLocked(runtime, TrainingJobStatus.STOPPED, forcedExit, "STOP_TIMEOUT", reason);
      releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
    }
    emitSyntheticControl(runtime, "stopped", reason);
    launchPromotedRuntimes(promoted);
  }

  private void onJobTimeout(ManagedJobRuntime runtime, long runGeneration, Duration timeout) {
    if (shuttingDown || !runtime.isActiveRunGeneration(runGeneration)) {
      return;
    }
    List<ScheduledLaunch> promoted = new ArrayList<>();
    String reason = "Training job exceeded maximum cumulative running duration of " + timeout + ".";
    synchronized (schedulerLock) {
      if (!runtime.isActiveRunGeneration(runGeneration) || runtime.statusEnum() != TrainingJobStatus.RUNNING) {
        return;
      }
      ManagedWorkerProcess terminatedProc = runtime.invalidateRunGenerationAndDestroyProcessLocked();
      Integer forcedExit = null;
      if (terminatedProc != null && !terminatedProc.isAlive()) {
        try {
          forcedExit = terminatedProc.exitValue();
        } catch (IllegalThreadStateException ignored) {
          // Process still terminating in OS.
        }
      }
      transitionJobLocked(runtime, TrainingJobStatus.FAILED, forcedExit, "JOB_TIMEOUT", reason);
      releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
    }
    emitSyntheticError(runtime, "JOB_TIMEOUT", reason);
    launchPromotedRuntimes(promoted);
  }

  private void readWorkerOutput(ManagedJobRuntime runtime, ManagedWorkerProcess process, long runGeneration) {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.outputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (shuttingDown || !runtime.isActiveRunGeneration(runGeneration)) {
          return;
        }
        handleWorkerLine(runtime, runGeneration, line.trim());
      }
    } catch (IOException ignored) {
      // Process exit handler reconciles final state and slot cleanup.
    }
  }

  private void handleWorkerLine(ManagedJobRuntime runtime, long runGeneration, String line) {
    if (line.isBlank() || !line.startsWith("{") || !runtime.isActiveRunGeneration(runGeneration)) {
      return;
    }
    JsonNode node;
    try {
      node = objectMapper.readTree(line);
    } catch (JsonProcessingException ex) {
      return;
    }
    if (!node.isObject()) {
      return;
    }
    ObjectNode eventNode = ((ObjectNode) node).deepCopy();
    String type = eventNode.path("type").asText("");
    String reportedDevice = eventNode.path("device").asText(null);
    if (reportedDevice != null && !reportedDevice.isBlank()) {
      runtime.updateDevice(runGeneration, reportedDevice.trim());
    }

    if ("metric".equals(type)) {
      try {
        TrainingMetricMessage metric = objectMapper.treeToValue(eventNode, TrainingMetricMessage.class);
        PendingEventWrite pending;
        synchronized (schedulerLock) {
          if (!runtime.isActiveRunGeneration(runGeneration)) {
            return;
          }
          runtime.updateMetrics(metric);
          pending = prepareRuntimeEventLocked(runtime, eventNode, Instant.now(), false);
        }
        persistAndBroadcastEvent(pending);
      } catch (JsonProcessingException ignored) {
        // Ignore malformed metric rows from the worker.
      }
    } else if ("control".equals(type)) {
      String workerStatus = eventNode.path("status").asText("");
      handleWorkerControlEventAndBroadcast(runtime, runGeneration, workerStatus, eventNode);
    } else if ("test_result".equals(type)) {
      synchronized (schedulerLock) {
        if (!runtime.isActiveRunGeneration(runGeneration)) {
          return;
        }
        runtime.setTestResult(eventNode);
      }
      if (!runtime.checkpointSaved() && runtime.owner() != null && !runtime.owner().isBlank()) {
        try {
          saveCheckpoint(runtime, eventNode);
          runtime.setCheckpointSaved(true);
        } catch (RuntimeException ignored) {
          // Keep streaming the test result even if persistence fails.
        }
      }
      appendAndBroadcastWorkerEvent(runtime, runGeneration, eventNode, true);
    } else if ("backprop".equals(type)) {
      appendAndBroadcastWorkerEvent(runtime, runGeneration, eventNode, false);
    } else if ("error".equals(type)) {
      String errorCode = eventNode.path("errorCode").asText("WORKER_RUNTIME_ERROR");
      String message = eventNode.path("message").asText("Python training worker failed.");
      synchronized (schedulerLock) {
        if (!runtime.isActiveRunGeneration(runGeneration)) {
          return;
        }
        runtime.recordWorkerError(errorCode, message);
      }
      appendAndBroadcastWorkerEvent(runtime, runGeneration, eventNode, true);
    }
  }

  private void handleWorkerControlEventAndBroadcast(
      ManagedJobRuntime runtime,
      long runGeneration,
      String workerStatus,
      ObjectNode eventNode
  ) {
    List<ScheduledLaunch> promoted = new ArrayList<>();
    PendingEventWrite pending;
    synchronized (schedulerLock) {
      if (!runtime.isActiveRunGeneration(runGeneration)) {
        return;
      }
      TrainingJobStatus current = runtime.statusEnum();
      if (!current.isTerminal() && workerStatus != null && !workerStatus.isBlank()) {
        switch (workerStatus) {
          case "paused" -> {
            if (current == TrainingJobStatus.RUNNING
                && TrainingJobStateMachine.canTransition(current, TrainingJobStatus.PAUSED)) {
              runtime.pauseJobTimeoutLocked();
              String nextPending = runtime.pendingControlEnum() == TrainingControlAction.STOP
                  ? TrainingControlAction.STOP.wireValue()
                  : null;
              transitionJobWithPendingLocked(runtime, TrainingJobStatus.PAUSED, nextPending, null, null, null);
              if (!TrainingJobStateMachine.occupiesExecutionSlot(
                  TrainingJobStatus.PAUSED, properties.isReleaseSlotOnPause())) {
                releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
              }
            }
          }
          case "running" -> {
            if (current == TrainingJobStatus.PAUSED
                && activeSlotJobIds.contains(runtime.jobId())
                && TrainingJobStateMachine.canTransition(current, TrainingJobStatus.RUNNING)) {
              String nextPending = runtime.pendingControlEnum() == TrainingControlAction.STOP
                  ? TrainingControlAction.STOP.wireValue()
                  : null;
              transitionJobWithPendingLocked(runtime, TrainingJobStatus.RUNNING, nextPending, null, null, null);
              resumeJobTimeoutLocked(runtime, runGeneration);
            }
          }
          case "completed" -> {
            if (TrainingJobStateMachine.canTransition(current, TrainingJobStatus.COMPLETED)) {
              transitionJobLocked(runtime, TrainingJobStatus.COMPLETED, null, null, null);
              releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
              scheduleTerminalProcessReaper(runtime, runGeneration, runtime.process());
            }
          }
          case "stopped" -> {
            if (TrainingJobStateMachine.canTransition(current, TrainingJobStatus.STOPPED)) {
              transitionJobLocked(runtime, TrainingJobStatus.STOPPED, null, null, null);
              releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
              scheduleTerminalProcessReaper(runtime, runGeneration, runtime.process());
            }
          }
          default -> {
            // Ignore unknown worker status values.
          }
        }
      }
      pending = prepareRuntimeEventLocked(runtime, eventNode, Instant.now(), true);
    }
    persistAndBroadcastEvent(pending);
    launchPromotedRuntimes(promoted);
  }

  private void scheduleTerminalProcessReaper(
      ManagedJobRuntime runtime,
      long runGeneration,
      ManagedWorkerProcess process
  ) {
    if (process == null || !process.isAlive()) {
      return;
    }
    long delayMs = Math.max(50L, Math.min(500L, properties.getStopTimeout().toMillis()));
    timerExecutor.schedule(() -> {
      if (!shuttingDown && process.isAlive()) {
        process.destroyForcibly();
      }
      synchronized (schedulerLock) {
        if (runtime.isActiveRunGeneration(runGeneration)) {
          runtime.clearProcessReference(process);
        }
        if (runtime.statusEnum().isTerminal()) {
          scheduleTerminalRuntimeEvictionLocked(runtime);
        }
      }
    }, delayMs, TimeUnit.MILLISECONDS);
  }

  private void waitForWorkerExit(
      ManagedJobRuntime runtime,
      ManagedWorkerProcess process,
      long runGeneration,
      Future<?> outputReaderFuture
  ) {
    try {
      int exitCode = process.waitFor();
      if (process.isAlive()) {
        process.destroyForcibly();
      }
      if (outputReaderFuture != null) {
        try {
          outputReaderFuture.get(2, TimeUnit.SECONDS);
        } catch (Exception ignored) {
          // Best-effort wait to drain trailing stdout lines before final state reconciliation.
        }
      }
      if (shuttingDown || !runtime.isActiveRunGeneration(runGeneration)) {
        return;
      }
      List<ScheduledLaunch> promoted = new ArrayList<>();
      String syntheticControlStatus = null;
      String syntheticControlMessage = null;
      String syntheticErrorCode = null;
      String syntheticErrorReason = null;
      synchronized (schedulerLock) {
        if (!runtime.isActiveRunGeneration(runGeneration)) {
          return;
        }
        runtime.clearProcessReference(process);
        TrainingJobStatus current = runtime.statusEnum();
        if (current.isTerminal()) {
          if (runtime.entity().getExitCode() == null) {
            runtime.entity().setExitCode(exitCode);
            runtime.entity().setUpdatedAt(Instant.now());
            jobRepository.saveAndFlush(runtime.entity());
            runtime.clearEntityDirtyLocked();
          }
          if (activeSlotJobIds.contains(runtime.jobId())) {
            releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
          }
          scheduleTerminalRuntimeEvictionLocked(runtime);
        } else if (exitCode == 0 && runtime.pendingControlEnum() == TrainingControlAction.STOP) {
          transitionJobLocked(runtime, TrainingJobStatus.STOPPED, 0, null, null);
          syntheticControlStatus = TrainingJobStatus.STOPPED.wireValue();
          syntheticControlMessage = "Training stopped.";
          releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
        } else if (exitCode == 0 && current == TrainingJobStatus.RUNNING && runtime.pendingControlEnum() == null) {
          transitionJobLocked(runtime, TrainingJobStatus.COMPLETED, 0, null, null);
          syntheticControlStatus = TrainingJobStatus.COMPLETED.wireValue();
          syntheticControlMessage = "Training completed.";
          releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
        } else {
          String defaultCode = exitCode == 0 ? "WORKER_UNEXPECTED_EXIT" : "WORKER_NON_ZERO_EXIT";
          String errCode = runtime.lastWorkerErrorCode() != null ? runtime.lastWorkerErrorCode() : defaultCode;
          String errReason = runtime.lastWorkerErrorMessage() != null
              ? runtime.lastWorkerErrorMessage()
              : "Training worker exited abnormally with code " + exitCode + ".";
          if (runtime.lastWorkerErrorCode() == null) {
            syntheticErrorCode = errCode;
            syntheticErrorReason = errReason;
          }
          transitionJobLocked(runtime, TrainingJobStatus.FAILED, exitCode, errCode, errReason);
          releaseSlotAndCollectNextLocked(runtime.jobId(), promoted);
        }
      }
      if (syntheticControlStatus != null) {
        emitSyntheticControl(runtime, syntheticControlStatus, syntheticControlMessage);
      }
      if (syntheticErrorCode != null) {
        emitSyntheticError(runtime, syntheticErrorCode, syntheticErrorReason);
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
    transitionJobWithPendingLocked(runtime, targetStatus, null, exitCode, errorCode, errorReason);
  }

  private void transitionJobWithPendingLocked(
      ManagedJobRuntime runtime,
      TrainingJobStatus targetStatus,
      String pendingControl,
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
    entity.setPendingControl(targetStatus.isTerminal() ? null : pendingControl);
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
      scheduleTerminalRuntimeEvictionLocked(runtime);
    }
    jobRepository.saveAndFlush(entity);
    runtime.clearEntityDirtyLocked();
  }

  private void scheduleTerminalRuntimeEvictionLocked(ManagedJobRuntime runtime) {
    if (shuttingDown) {
      return;
    }
    Duration retention = properties.getTerminalRuntimeRetention();
    if (retention == null || retention.isNegative()) {
      return;
    }
    long delayMs = Math.max(1L, retention.toMillis());
    ScheduledFuture<?> future = timerExecutor.schedule(
        () -> evictTerminalRuntimeIfIdle(runtime),
        delayMs,
        TimeUnit.MILLISECONDS
    );
    runtime.setEvictionFuture(future);
  }

  private void evictTerminalRuntimeIfIdle(ManagedJobRuntime runtime) {
    if (shuttingDown) {
      return;
    }
    runtime.awaitPendingEventWrites();
    synchronized (schedulerLock) {
      if (shuttingDown
          || runtimes.get(runtime.jobId()) != runtime
          || !runtime.statusEnum().isTerminal()) {
        return;
      }
      ManagedWorkerProcess proc = runtime.process();
      if (proc != null && proc.isAlive()) {
        return;
      }
      if (runtime.isEntityDirtyLocked()) {
        jobRepository.saveAndFlush(runtime.entity());
        runtime.clearEntityDirtyLocked();
      }
      runtimes.remove(runtime.jobId(), runtime);
    }
    CopyOnWriteArraySet<WebSocketSession> jobSessions = sessions.get(runtime.jobId());
    if (jobSessions != null) {
      jobSessions.removeIf(s -> !s.isOpen());
      if (jobSessions.isEmpty()) {
        sessions.remove(runtime.jobId(), jobSessions);
      }
    }
  }

  private void releaseSlotAndCollectNextLocked(String finishedJobId, List<ScheduledLaunch> promotedOut) {
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
      candidate.clearEntityDirtyLocked();
      activeSlotJobIds.add(nextJobId);
      promotedOut.add(new ScheduledLaunch(candidate, candidate.allocateRunGenerationLocked()));
    }
  }

  private void launchPromotedRuntimes(List<ScheduledLaunch> promoted) {
    for (ScheduledLaunch next : promoted) {
      launchWorkerForSlotOwner(next, false);
    }
  }

  private void emitSyntheticControl(ManagedJobRuntime runtime, String status, String message) {
    ObjectNode node = objectMapper.createObjectNode();
    node.put("type", "control");
    node.put("status", status);
    node.put("message", message);
    appendAndBroadcastEvent(runtime, node);
  }

  private void emitSyntheticError(ManagedJobRuntime runtime, String errorCode, String message) {
    ObjectNode node = objectMapper.createObjectNode();
    node.put("type", "error");
    node.put("errorCode", errorCode);
    node.put("message", message);
    appendAndBroadcastEvent(runtime, node);
  }

  private void appendAndBroadcastWorkerEvent(
      ManagedJobRuntime runtime,
      long runGeneration,
      ObjectNode eventNode,
      boolean flushJobEntityNow
  ) {
    PendingEventWrite pending;
    synchronized (schedulerLock) {
      if (!runtime.isActiveRunGeneration(runGeneration)) {
        return;
      }
      pending = prepareRuntimeEventLocked(runtime, eventNode, Instant.now(), flushJobEntityNow);
    }
    persistAndBroadcastEvent(pending);
  }

  private void appendAndBroadcastEvent(ManagedJobRuntime runtime, ObjectNode eventNode) {
    PendingEventWrite pending;
    synchronized (schedulerLock) {
      pending = prepareRuntimeEventLocked(runtime, eventNode, Instant.now(), true);
    }
    persistAndBroadcastEvent(pending);
  }

  private PendingEventWrite prepareRuntimeEventLocked(
      ManagedJobRuntime runtime,
      ObjectNode eventNode,
      Instant now,
      boolean flushJobEntityNow
  ) {
    TrainingJob entity = runtime.entity();
    long nextSeq = entity.getLastSequence() + 1L;
    entity.setLastSequence(nextSeq);
    entity.setUpdatedAt(now);
    boolean periodicFlush = runtime.markEntityDirtyAndShouldFlushLocked(ENTITY_FLUSH_EVENT_INTERVAL);
    if (flushJobEntityNow || periodicFlush || runtime.statusEnum().isTerminal()) {
      jobRepository.saveAndFlush(entity);
      runtime.clearEntityDirtyLocked();
    }

    String jobId = entity.getJobId();
    String eventType = eventNode.path("type").asText("unknown");
    eventNode.put("jobId", jobId);
    eventNode.put("sequence", nextSeq);
    eventNode.put("timestamp", now.toString());
    eventNode.put("schemaVersion", EVENT_SCHEMA_VERSION);

    ObjectNode snapshot = eventNode.deepCopy();
    String rawJson = snapshot.toString();
    runtime.addSequencedEvent(new SequencedEventRecord(nextSeq, snapshot, rawJson));

    if (eventRepository != null) {
      TrainingJobEvent eventRow = new TrainingJobEvent(
          jobId,
          nextSeq,
          eventType,
          EVENT_SCHEMA_VERSION,
          now,
          rawJson
      );
      runtime.incrementPendingEventWrites();
      return new PendingEventWrite(runtime, eventRow, rawJson, runtime.statusEnum().isTerminal());
    }
    return new PendingEventWrite(runtime, null, rawJson, false);
  }

  private void persistAndBroadcastEvent(PendingEventWrite pending) {
    if (pending.eventRow() != null && eventRepository != null) {
      ManagedJobRuntime runtime = pending.runtime();
      synchronized (runtime.persistenceLock()) {
        try {
          eventRepository.save(pending.eventRow());
          int maxHistory = properties.getMaxHistoryEvents();
          long seq = pending.eventRow().getSequence();
          long targetPruneFloor = seq - maxHistory;
          if (targetPruneFloor > runtime.lastPrunedUpToSequence()) {
            long unprunedOverflow = targetPruneFloor - runtime.lastPrunedUpToSequence();
            if (pending.terminalEvent() || unprunedOverflow >= EVENT_PRUNE_BATCH_SIZE) {
              eventRepository.deleteByJobIdAndSequenceLessThanEqual(runtime.jobId(), targetPruneFloor);
              runtime.setLastPrunedUpToSequence(targetPruneFloor);
            }
          }
        } finally {
          runtime.decrementPendingEventWritesInPersistenceLock();
        }
      }
    }
    broadcastRaw(pending.runtime().jobId(), pending.rawJson());
  }

  private String recordEntityEventLocked(TrainingJob entity, ObjectNode eventNode, Instant now) {
    long nextSeq = entity.getLastSequence() + 1L;
    entity.setLastSequence(nextSeq);
    entity.setUpdatedAt(now);
    jobRepository.saveAndFlush(entity);

    String jobId = entity.getJobId();
    String eventType = eventNode.path("type").asText("unknown");
    eventNode.put("jobId", jobId);
    eventNode.put("sequence", nextSeq);
    eventNode.put("timestamp", now.toString());
    eventNode.put("schemaVersion", EVENT_SCHEMA_VERSION);

    ObjectNode snapshot = eventNode.deepCopy();
    String rawJson = snapshot.toString();
    if (eventRepository != null) {
      eventRepository.saveAndFlush(new TrainingJobEvent(
          jobId,
          nextSeq,
          eventType,
          EVENT_SCHEMA_VERSION,
          now,
          rawJson
      ));
      int maxHistory = properties.getMaxHistoryEvents();
      if (nextSeq > maxHistory) {
        eventRepository.deleteByJobIdAndSequenceLessThanEqual(jobId, nextSeq - maxHistory);
      }
    }
    return rawJson;
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

  private ManagedJobRuntime resolveRuntimeForControl(String jobId, TrainingControlAction action) {
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      return runtime;
    }
    TrainingJob entity = jobRepository.findById(jobId)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    TrainingJobStateMachine.validateControlRequest(
        entity.getJobId(),
        TrainingJobStatus.fromWireValue(entity.getStatus()),
        TrainingControlAction.fromNullableWireValue(entity.getPendingControl()),
        action
    );
    throw new IllegalArgumentException("Training job runtime is not active.");
  }

  private ManagedJobRuntime resolveOwnedRuntimeForControl(String username, String jobId, TrainingControlAction action) {
    requireUser(username);
    ManagedJobRuntime runtime = runtimes.get(jobId);
    if (runtime != null) {
      if (runtime.owner() == null || !runtime.owner().equals(username)) {
        throw new IllegalArgumentException("Training job not found.");
      }
      return runtime;
    }
    TrainingJob entity = jobRepository.findByJobIdAndOwner(jobId, username)
        .orElseThrow(() -> new IllegalArgumentException("Training job not found."));
    TrainingJobStateMachine.validateControlRequest(
        entity.getJobId(),
        TrainingJobStatus.fromWireValue(entity.getStatus()),
        TrainingControlAction.fromNullableWireValue(entity.getPendingControl()),
        action
    );
    throw new IllegalArgumentException("Training job runtime is not active.");
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

  private StartTrainingRequest deserializeRequest(String rawJson) {
    if (rawJson == null || rawJson.isBlank()) {
      throw new IllegalArgumentException("Original training request payload is unavailable for reset.");
    }
    try {
      return objectMapper.readValue(rawJson, StartTrainingRequest.class);
    } catch (JsonProcessingException ex) {
      throw new IllegalArgumentException("Failed to deserialize original training request for reset.");
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

  private String normalizeIdempotencyKey(String rawKey) {
    if (rawKey == null) {
      return null;
    }
    String trimmed = rawKey.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (trimmed.length() > 128) {
      throw new IllegalArgumentException("Idempotency-Key must not exceed 128 characters.");
    }
    return trimmed;
  }

  private Integer queuePositionLocked(String jobId) {
    int pos = 1;
    for (String queuedId : waitQueue) {
      if (Objects.equals(queuedId, jobId)) {
        return pos;
      }
      pos++;
    }
    return null;
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

  private record SequencedEventRecord(long sequence, JsonNode payload, String rawJson) {}

  private record ScheduledLaunch(ManagedJobRuntime runtime, long runGeneration) {}

  private record PendingEventWrite(
      ManagedJobRuntime runtime,
      TrainingJobEvent eventRow,
      String rawJson,
      boolean terminalEvent
  ) {}

  private final class ManagedJobRuntime {
    private final TrainingJob entity;
    private final StartTrainingRequest request;
    private final Path directory;
    private final Path requestFile;
    private final Path controlFile;
    private final Path checkpointFile;
    private final Object persistenceLock = new Object();
    private int pendingEventWrites = 0;
    private long lastPrunedUpToSequence = 0L;
    private int dirtyEntityEvents = 0;
    private volatile long runGeneration = 0L;
    private volatile TrainingMetricMessage latestMetric;
    private volatile JsonNode testResult;
    private volatile boolean checkpointSaved;
    private volatile ManagedWorkerProcess process;
    private volatile ScheduledFuture<?> jobTimeoutFuture;
    private volatile ScheduledFuture<?> stopTimeoutFuture;
    private volatile ScheduledFuture<?> evictionFuture;
    private long remainingJobTimeoutNanos = -1L;
    private long activeRunStartedAtNanos = 0L;
    private boolean runningSegmentActive = false;
    private volatile String lastWorkerErrorCode;
    private volatile String lastWorkerErrorMessage;
    private final ArrayDeque<SequencedEventRecord> recentEvents = new ArrayDeque<>();
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

    private void updateDevice(long expectedGeneration, String device) {
      synchronized (schedulerLock) {
        if (!isActiveRunGeneration(expectedGeneration)) {
          return;
        }
        if (!Objects.equals(entity.getDevice(), device)) {
          entity.setDevice(device);
          entity.setUpdatedAt(Instant.now());
          jobRepository.saveAndFlush(entity);
          clearEntityDirtyLocked();
        }
      }
    }

    private void recordWorkerError(String errorCode, String message) {
      this.lastWorkerErrorCode = errorCode;
      this.lastWorkerErrorMessage = message;
    }

    private void addSequencedEvent(SequencedEventRecord record) {
      int maxEvents = properties.getMaxHistoryEvents();
      synchronized (recentEvents) {
        recentEvents.addLast(record);
        while (recentEvents.size() > maxEvents) {
          recentEvents.removeFirst();
        }
      }
    }

    private List<String> recentStreamEvents() {
      synchronized (recentEvents) {
        List<String> out = new ArrayList<>(recentEvents.size());
        for (SequencedEventRecord record : recentEvents) {
          out.add(record.rawJson());
        }
        return out;
      }
    }

    private Long earliestSequence() {
      synchronized (recentEvents) {
        SequencedEventRecord first = recentEvents.peekFirst();
        return first == null ? null : first.sequence();
      }
    }

    private List<JsonNode> eventsAfter(long afterSequence, int limit) {
      synchronized (recentEvents) {
        List<JsonNode> out = new ArrayList<>();
        for (SequencedEventRecord record : recentEvents) {
          if (record.sequence() > afterSequence) {
            out.add(record.payload());
            if (out.size() >= limit) {
              break;
            }
          }
        }
        return out;
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

    private long allocateRunGenerationLocked() {
      runGeneration += 1L;
      return runGeneration;
    }

    private long currentRunGenerationLocked() {
      return runGeneration;
    }

    private boolean isActiveRunGeneration(long expectedGeneration) {
      return expectedGeneration > 0L && runGeneration == expectedGeneration;
    }

    private void attachProcessLocked(long expectedGeneration, ManagedWorkerProcess nextProcess) {
      if (isActiveRunGeneration(expectedGeneration)) {
        this.process = nextProcess;
      }
    }

    private ManagedWorkerProcess invalidateRunGenerationAndDestroyProcessLocked() {
      runGeneration += 1L;
      cancelTimers();
      ManagedWorkerProcess current = process;
      process = null;
      if (current != null) {
        current.destroyForcibly();
      }
      return current;
    }

    private void initializeJobTimeoutLocked(long totalTimeoutNanos) {
      this.remainingJobTimeoutNanos = totalTimeoutNanos;
      this.activeRunStartedAtNanos = System.nanoTime();
      this.runningSegmentActive = true;
    }

    private void pauseJobTimeoutLocked() {
      ScheduledFuture<?> jf = jobTimeoutFuture;
      jobTimeoutFuture = null;
      if (jf != null) {
        jf.cancel(false);
      }
      if (runningSegmentActive && remainingJobTimeoutNanos > 0L) {
        long elapsedNanos = Math.max(0L, System.nanoTime() - activeRunStartedAtNanos);
        remainingJobTimeoutNanos = Math.max(1L, remainingJobTimeoutNanos - elapsedNanos);
      }
      runningSegmentActive = false;
    }

    private void markJobTimeoutResumedLocked(long fallbackTotalNanos) {
      if (remainingJobTimeoutNanos < 0L) {
        remainingJobTimeoutNanos = fallbackTotalNanos;
      }
      this.activeRunStartedAtNanos = System.nanoTime();
      this.runningSegmentActive = true;
    }

    private long remainingJobTimeoutNanosLocked() {
      return remainingJobTimeoutNanos;
    }

    private void cancelTimers() {
      runningSegmentActive = false;
      ScheduledFuture<?> jf = jobTimeoutFuture;
      jobTimeoutFuture = null;
      if (jf != null) {
        jf.cancel(false);
      }
      ScheduledFuture<?> sf = stopTimeoutFuture;
      stopTimeoutFuture = null;
      if (sf != null) {
        sf.cancel(false);
      }
      ScheduledFuture<?> ef = evictionFuture;
      evictionFuture = null;
      if (ef != null) {
        ef.cancel(false);
      }
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
    private void setJobTimeoutFuture(ScheduledFuture<?> future) {
      ScheduledFuture<?> prev = this.jobTimeoutFuture;
      this.jobTimeoutFuture = future;
      if (prev != null) {
        prev.cancel(false);
      }
    }
    private void setStopTimeoutFuture(ScheduledFuture<?> future) {
      ScheduledFuture<?> prev = this.stopTimeoutFuture;
      this.stopTimeoutFuture = future;
      if (prev != null) {
        prev.cancel(false);
      }
    }
    private void setEvictionFuture(ScheduledFuture<?> future) {
      ScheduledFuture<?> prev = this.evictionFuture;
      this.evictionFuture = future;
      if (prev != null) {
        prev.cancel(false);
      }
    }
    private String lastWorkerErrorCode() { return lastWorkerErrorCode; }
    private String lastWorkerErrorMessage() { return lastWorkerErrorMessage; }
    private Object persistenceLock() { return persistenceLock; }
    private long lastPrunedUpToSequence() { return lastPrunedUpToSequence; }
    private void setLastPrunedUpToSequence(long seq) { this.lastPrunedUpToSequence = seq; }
    private boolean markEntityDirtyAndShouldFlushLocked(int interval) {
      dirtyEntityEvents += 1;
      return dirtyEntityEvents >= interval;
    }
    private boolean isEntityDirtyLocked() { return dirtyEntityEvents > 0; }
    private void clearEntityDirtyLocked() { dirtyEntityEvents = 0; }
    private void incrementPendingEventWrites() {
      synchronized (persistenceLock) {
        pendingEventWrites += 1;
      }
    }
    private void decrementPendingEventWritesInPersistenceLock() {
      pendingEventWrites = Math.max(0, pendingEventWrites - 1);
      if (pendingEventWrites == 0) {
        persistenceLock.notifyAll();
      }
    }
    private void awaitPendingEventWrites() {
      synchronized (persistenceLock) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (pendingEventWrites > 0) {
          long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
          if (remainingMillis <= 0L) {
            break;
          }
          try {
            persistenceLock.wait(remainingMillis);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
  }
}
