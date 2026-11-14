package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.ControllableTrainingProcessLauncher.FakeWorkerProcess;
import com.deepvision.studio.training.TrainingDtos.TrainingStartResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingStatusResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@DataJpaTest
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class TrainingFailureAndRecoveryTest {
  @Autowired
  private TrainingJobRepository jobRepository;

  @Autowired
  private AppUserRepository userRepository;

  @Autowired
  private TrainingCheckpointRepository checkpointRepository;

  @TempDir
  Path tempDir;

  private TrainingJobService activeService;

  @AfterEach
  void tearDown() {
    if (activeService != null) {
      activeService.shutdown();
    }
    checkpointRepository.deleteAll();
    jobRepository.deleteAll();
    userRepository.deleteAll();
  }

  @Test
  void forcedWorkerKillMarksJobFailedWithExitCodeAndReasonReleasesSlotAndStartsQueuedJob() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-kill").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(3);
    props.setMaxActiveJobsPerUser(5);

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    activeService = new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        (req, ds) -> {},
        launcher,
        props,
        new ObjectMapper(),
        tempDir.resolve("datasets").toString()
    );

    TrainingStartResponse first = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 5), "alice");
    TrainingStartResponse second = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 5), "bob");
    assertThat(first.status()).isEqualTo("running");
    assertThat(second.status()).isEqualTo("queued");

    FakeWorkerProcess worker1 = launcher.awaitProcess(first.jobId(), Duration.ofSeconds(2));
    // Simulate external forced kill (SIGKILL / TerminateProcess -> exit code 137)
    worker1.finish(137);

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "failed".equals(activeService.status("alice", first.jobId()).status())
            && "running".equals(activeService.status("bob", second.jobId()).status()),
        "Expected killed worker job to become failed and next queued job to start"
    );

    TrainingStatusResponse failedStatus = activeService.status("alice", first.jobId());
    assertThat(failedStatus.exitCode()).isEqualTo(137);
    assertThat(failedStatus.errorCode()).isEqualTo("WORKER_NON_ZERO_EXIT");
    assertThat(failedStatus.errorReason()).contains("137");
    assertThat(failedStatus.finishedAt()).isNotNull();

    // Finish second job and verify zero occupied slots and zero residual managed processes
    FakeWorkerProcess worker2 = launcher.awaitProcess(second.jobId(), Duration.ofSeconds(2));
    worker2.completeCleanly();

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(activeService.status("bob", second.jobId()).status())
            && activeService.activeSlotCount() == 0
            && activeService.activeManagedProcessCount() == 0,
        "Expected all slots and managed processes to be cleaned up after completion"
    );
    assertThat(launcher.liveProcessCount()).isEqualTo(0);
  }

  @Test
  void handlesLaunchFailureAndJobTimeoutWithoutLeakingSlotsOrProcesses() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-launch-fail").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(3);
    props.setMaxActiveJobsPerUser(5);
    props.setJobTimeout(Duration.ofMillis(150));

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    AtomicBoolean failLaunch = new AtomicBoolean(true);
    launcher.setFailureInjector(req -> failLaunch.get() ? new IOException("Python executable not found") : null);

    activeService = new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        (req, ds) -> {},
        launcher,
        props,
        new ObjectMapper(),
        tempDir.resolve("datasets").toString()
    );

    assertThatThrownBy(() -> activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "alice"))
        .isInstanceOf(TrainingApiException.class)
        .satisfies(ex -> assertThat(((TrainingApiException) ex).code()).isEqualTo("WORKER_START_FAILED"));

    assertThat(activeService.activeSlotCount()).isEqualTo(0);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(0);
    TrainingJob failedLaunchJob = jobRepository.findAll().get(0);
    assertThat(failedLaunchJob.getStatus()).isEqualTo("failed");
    assertThat(failedLaunchJob.getErrorCode()).isEqualTo("WORKER_START_FAILED");

    // Clear launch failure -> start a hung job that hits jobTimeout -> slot is released cleanly
    failLaunch.set(false);
    TrainingStartResponse timedOutJob = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "alice");
    FakeWorkerProcess hungProcess = launcher.awaitProcess(timedOutJob.jobId(), Duration.ofSeconds(2));

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "failed".equals(activeService.status("alice", timedOutJob.jobId()).status()),
        "Expected hung job to fail upon jobTimeout"
    );
    TrainingStatusResponse timeoutStatus = activeService.status("alice", timedOutJob.jobId());
    assertThat(hungProcess.wasForciblyDestroyed()).isTrue();
    assertThat(timeoutStatus.errorCode()).isEqualTo("JOB_TIMEOUT");
    assertThat(activeService.activeSlotCount()).isEqualTo(0);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(0);
    assertThat(launcher.liveProcessCount()).isEqualTo(0);
  }

  @Test
  void runtimeExceptionDuringWorkerLaunchMarksJobFailedReleasesSlotAndPromotesQueuedJob() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-runtime-ex").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(3);
    props.setMaxActiveJobsPerUser(5);

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    activeService = new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        (req, ds) -> {},
        launcher,
        props,
        new ObjectMapper(),
        tempDir.resolve("datasets").toString()
    );

    // 1. Direct start() throwing RuntimeException must mark job failed, emit synthetic error, and release slot
    AtomicBoolean failDirectRuntime = new AtomicBoolean(true);
    launcher.setRuntimeFailureInjector(
        req -> failDirectRuntime.get() ? new IllegalStateException("Invalid ProcessBuilder environment") : null
    );

    assertThatThrownBy(() -> activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "alice"))
        .isInstanceOf(TrainingApiException.class)
        .satisfies(ex -> assertThat(((TrainingApiException) ex).code()).isEqualTo("WORKER_START_FAILED"));

    assertThat(activeService.activeSlotCount()).isEqualTo(0);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(0);
    TrainingJob directFailed = jobRepository.findAll().get(0);
    assertThat(directFailed.getStatus()).isEqualTo("failed");
    assertThat(directFailed.getErrorCode()).isEqualTo("WORKER_START_FAILED");
    assertThat(directFailed.getErrorReason()).contains("Invalid ProcessBuilder environment");
    var directEvents = activeService.events("alice", directFailed.getJobId(), 0L, 10);
    assertThat(directEvents.events()).anyMatch(
        ev -> "error".equals(ev.path("type").asText()) && "WORKER_START_FAILED".equals(ev.path("errorCode").asText())
    );

    // 2. Promoted queued job throwing RuntimeException must fail cleanly, release slot, and auto-promote next queued job
    failDirectRuntime.set(false);
    TrainingStartResponse first = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "alice");
    TrainingStartResponse second = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "bob");
    TrainingStartResponse third = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "carol");
    assertThat(first.status()).isEqualTo("running");
    assertThat(second.status()).isEqualTo("queued");
    assertThat(third.status()).isEqualTo("queued");

    launcher.setRuntimeFailureInjector(
        req -> second.jobId().equals(req.jobId()) ? new IllegalArgumentException("Bad command argument") : null
    );

    // Completing first job promotes second (throws RuntimeException -> failed), which immediately promotes third!
    launcher.awaitProcess(first.jobId(), Duration.ofSeconds(2)).completeCleanly();

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "failed".equals(activeService.status("bob", second.jobId()).status())
            && "running".equals(activeService.status("carol", third.jobId()).status()),
        "Expected RuntimeException on promoted job2 to fail job2 and promote queued job3 to running"
    );

    TrainingStatusResponse secondStatus = activeService.status("bob", second.jobId());
    assertThat(secondStatus.errorCode()).isEqualTo("WORKER_START_FAILED");
    assertThat(secondStatus.errorReason()).contains("Bad command argument");
    assertThat(activeService.activeSlotCount()).isEqualTo(1);

    launcher.awaitProcess(third.jobId(), Duration.ofSeconds(2)).completeCleanly();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(activeService.status("carol", third.jobId()).status())
            && activeService.activeSlotCount() == 0,
        "Expected third job to complete and release slot"
    );
  }

  @Test
  void serviceRestartMarksOrphanedRunningPausedAndQueuedJobsAsInterrupted() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-restart").toString());
    props.setMaxConcurrentJobs(2);
    props.setMaxQueueSize(3);
    props.setMaxActiveJobsPerUser(10);

    ControllableTrainingProcessLauncher firstLauncher = new ControllableTrainingProcessLauncher();
    TrainingJobService firstInstance = new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        (req, ds) -> {},
        firstLauncher,
        props,
        new ObjectMapper(),
        tempDir.resolve("datasets").toString()
    );

    // Create 1 completed job, 1 running job, 1 paused job, and 1 queued job in first service instance
    TrainingStartResponse completedJob = firstInstance.start(TrainingTestFixtures.sampleStartRequest(32, 2), "alice");
    firstLauncher.awaitProcess(completedJob.jobId(), Duration.ofSeconds(2)).completeCleanly();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(firstInstance.status("alice", completedJob.jobId()).status()),
        "Expected completedJob to finish"
    );

    TrainingStartResponse runningJob = firstInstance.start(TrainingTestFixtures.sampleStartRequest(32, 6), "alice");
    TrainingStartResponse pausedJob = firstInstance.start(TrainingTestFixtures.sampleStartRequest(32, 6), "bob");
    FakeWorkerProcess pausedWorker = firstLauncher.awaitProcess(pausedJob.jobId(), Duration.ofSeconds(2));
    firstInstance.pause("bob", pausedJob.jobId());
    pausedWorker.emitControl("paused", "Paused.");
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "paused".equals(firstInstance.status("bob", pausedJob.jobId()).status()),
        "Expected pausedJob to confirm pause"
    );

    TrainingStartResponse queuedJob = firstInstance.start(TrainingTestFixtures.sampleStartRequest(32, 6), "carol");
    assertThat(queuedJob.status()).isEqualTo("queued");

    // Simulate service shutdown / crash
    firstInstance.shutdown();

    // Boot a new TrainingJobService instance over the same H2 repository
    ControllableTrainingProcessLauncher secondLauncher = new ControllableTrainingProcessLauncher();
    activeService = new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        (req, ds) -> {},
        secondLauncher,
        props,
        new ObjectMapper(),
        tempDir.resolve("datasets").toString()
    );

    assertThat(activeService.status("alice", completedJob.jobId()).status()).isEqualTo("completed");

    for (var pair : java.util.List.of(
        java.util.Map.entry("alice", runningJob.jobId()),
        java.util.Map.entry("bob", pausedJob.jobId()),
        java.util.Map.entry("carol", queuedJob.jobId())
    )) {
      TrainingStatusResponse recovered = activeService.status(pair.getKey(), pair.getValue());
      assertThat(recovered.status()).isEqualTo("interrupted");
      assertThat(recovered.errorCode()).isEqualTo("SERVICE_RESTART_INTERRUPTED");
      assertThat(recovered.errorReason()).contains("Service restarted");
      assertThat(recovered.pendingControl()).isNull();
      assertThat(recovered.finishedAt()).isNotNull();
    }

    // Calling resume on an interrupted job returns HTTP 409 INVALID_STATE_TRANSITION
    assertThatThrownBy(() -> activeService.resume("bob", pausedJob.jobId()))
        .isInstanceOf(TrainingStateConflictException.class)
        .satisfies(ex -> assertThat(((TrainingStateConflictException) ex).httpStatus()).isEqualTo(HttpStatus.CONFLICT));

    // Fresh jobs can be scheduled normally after recovery
    assertThat(activeService.activeSlotCount()).isEqualTo(0);
    TrainingStartResponse afterRestart = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 2), "alice");
    assertThat(afterRestart.status()).isEqualTo("running");
  }

  @Test
  void jobTimeoutAccumulatesOnlyRunningDurationAndStopsCountingWhilePaused() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-pause-timeout").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(4);
    props.setJobTimeout(Duration.ofMillis(280));

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    activeService = new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        (req, ds) -> {},
        launcher,
        props,
        new ObjectMapper(),
        tempDir.resolve("datasets").toString()
    );

    TrainingStartResponse started = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 8), "alice");
    FakeWorkerProcess worker = launcher.awaitProcess(started.jobId(), Duration.ofSeconds(2));

    // Consume ~90ms of running budget (~190ms remaining)
    Thread.sleep(90);

    activeService.pause("alice", started.jobId());
    worker.emitControl("paused", "Paused during lecture.");
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "paused".equals(activeService.status("alice", started.jobId()).status()),
        "Expected job to enter paused status"
    );

    // Stay paused for 380ms (well past original 280ms wall-clock timeout); must NOT time out while paused!
    Thread.sleep(380);
    assertThat(activeService.status("alice", started.jobId()).status()).isEqualTo("paused");
    assertThat(activeService.status("alice", started.jobId()).errorCode()).isNull();
    assertThat(worker.wasForciblyDestroyed()).isFalse();

    // Resume -> remaining ~190ms running budget resumes and expires shortly after
    activeService.resume("alice", started.jobId());
    worker.emitControl("running", "Resumed.");
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "running".equals(activeService.status("alice", started.jobId()).status()),
        "Expected job to resume running"
    );

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "failed".equals(activeService.status("alice", started.jobId()).status()),
        "Expected job to fail with JOB_TIMEOUT once remaining active budget expires"
    );
    TrainingStatusResponse timedOut = activeService.status("alice", started.jobId());
    assertThat(timedOut.errorCode()).isEqualTo("JOB_TIMEOUT");
    assertThat(timedOut.errorReason()).contains("cumulative running duration");
    assertThat(worker.wasForciblyDestroyed()).isTrue();
    assertThat(activeService.activeSlotCount()).isEqualTo(0);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(0);
  }
}
