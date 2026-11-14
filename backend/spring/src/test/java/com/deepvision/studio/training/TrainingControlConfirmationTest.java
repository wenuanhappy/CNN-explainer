package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.deepvision.studio.auth.AppUser;
import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.ControllableTrainingProcessLauncher.FakeWorkerProcess;
import com.deepvision.studio.training.TrainingDtos.TrainingControlResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingEventsResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingStartResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingStatusResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
class TrainingControlConfirmationTest {
  @Autowired
  private TrainingJobRepository jobRepository;

  @Autowired
  private AppUserRepository userRepository;

  @Autowired
  private TrainingCheckpointRepository checkpointRepository;

  @Autowired
  private TrainingJobEventRepository eventRepository;

  @TempDir
  Path tempDir;

  private TrainingJobService activeService;

  @AfterEach
  void tearDown() {
    if (activeService != null) {
      activeService.shutdown();
    }
    eventRepository.deleteAll();
    checkpointRepository.deleteAll();
    jobRepository.deleteAll();
    userRepository.deleteAll();
  }

  @Test
  void pauseResumeAndStopRequireWorkerConfirmationAndPausedJobKeepsSlot() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-control").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(3);
    props.setMaxActiveJobsPerUser(5);
    props.setStopTimeout(Duration.ofSeconds(5));

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

    TrainingStartResponse started = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 6), "alice");
    String jobId = started.jobId();
    FakeWorkerProcess worker = launcher.awaitProcess(jobId, Duration.ofSeconds(2));

    // 1. Pause only accepts and writes control.json; status stays 'running' until worker confirms
    TrainingControlResponse pauseResp = activeService.pause("alice", jobId);
    assertThat(pauseResp.accepted()).isTrue();
    assertThat(pauseResp.status()).isEqualTo("running");
    assertThat(pauseResp.pendingControl()).isEqualTo("pause");
    assertThat(worker.readControlCommand()).isEqualTo("paused");
    assertThat(activeService.status("alice", jobId).status()).isEqualTo("running");

    worker.emitControl("paused", "Training paused at batch boundary.");
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "paused".equals(activeService.status("alice", jobId).status()),
        "Expected job to transition to paused after worker confirmation"
    );
    assertThat(activeService.status("alice", jobId).pendingControl()).isNull();

    // Paused job continues occupying execution slot by default -> second job enters queue
    TrainingStartResponse secondJob = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "bob");
    assertThat(secondJob.status()).isEqualTo("queued");
    assertThat(activeService.activeSlotCount()).isEqualTo(1);

    // 2. Resume only accepts; status stays 'paused' until worker confirms 'running'
    TrainingControlResponse resumeResp = activeService.resume("alice", jobId);
    assertThat(resumeResp.accepted()).isTrue();
    assertThat(resumeResp.status()).isEqualTo("paused");
    assertThat(resumeResp.pendingControl()).isEqualTo("resume");
    assertThat(worker.readControlCommand()).isEqualTo("running");
    assertThat(activeService.status("alice", jobId).status()).isEqualTo("paused");

    worker.emitControl("running", "Training resumed.");
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "running".equals(activeService.status("alice", jobId).status()),
        "Expected job to transition back to running after worker confirmation"
    );
    assertThat(activeService.status("alice", jobId).pendingControl()).isNull();

    // 3. Stop only accepts; status stays 'running' until worker confirms 'stopped', then promotes bob's queued job
    TrainingControlResponse stopResp = activeService.stop("alice", jobId);
    assertThat(stopResp.accepted()).isTrue();
    assertThat(stopResp.status()).isEqualTo("running");
    assertThat(stopResp.pendingControl()).isEqualTo("stop");
    assertThat(worker.readControlCommand()).isEqualTo("stopped");
    assertThat(worker.wasForciblyDestroyed()).isFalse();
    assertThat(activeService.status("alice", jobId).status()).isEqualTo("running");

    worker.confirmStopAndExit();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "stopped".equals(activeService.status("alice", jobId).status())
            && "running".equals(activeService.status("bob", secondJob.jobId()).status()),
        "Expected stopped confirmation to finalize job and promote queued job"
    );
    assertThat(activeService.status("alice", jobId).pendingControl()).isNull();
  }

  @Test
  void resumeOnCompletedJobReturnsConflict409AndStopTimeoutForciblyTerminatesHungWorker() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-control-timeout").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(5);
    props.setStopTimeout(Duration.ofMillis(120));

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

    // Job 1 completes cleanly -> calling resume must throw HTTP 409 INVALID_STATE_TRANSITION
    TrainingStartResponse job1 = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 2), "alice");
    FakeWorkerProcess worker1 = launcher.awaitProcess(job1.jobId(), Duration.ofSeconds(2));
    worker1.completeCleanly();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(activeService.status("alice", job1.jobId()).status()),
        "Expected job1 to become completed"
    );

    assertThatThrownBy(() -> activeService.resume("alice", job1.jobId()))
        .isInstanceOf(TrainingStateConflictException.class)
        .satisfies(ex -> {
          TrainingStateConflictException conflict = (TrainingStateConflictException) ex;
          assertThat(conflict.httpStatus()).isEqualTo(HttpStatus.CONFLICT);
          assertThat(conflict.code()).isEqualTo("INVALID_STATE_TRANSITION");
        });

    // Job 2 ignores stop control -> stopTimeout forcibly kills worker and releases execution slot
    TrainingStartResponse job2 = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "alice");
    FakeWorkerProcess worker2 = launcher.awaitProcess(job2.jobId(), Duration.ofSeconds(2));
    TrainingControlResponse stopAccepted = activeService.stop("alice", job2.jobId());
    assertThat(stopAccepted.status()).isEqualTo("running");
    assertThat(stopAccepted.pendingControl()).isEqualTo("stop");

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "stopped".equals(activeService.status("alice", job2.jobId()).status()),
        "Expected hung worker to be forcibly stopped after stopTimeout"
    );

    TrainingStatusResponse finalStatus = activeService.status("alice", job2.jobId());
    assertThat(worker2.wasForciblyDestroyed()).isTrue();
    assertThat(finalStatus.errorCode()).isEqualTo("STOP_TIMEOUT");
    assertThat(finalStatus.errorReason()).contains("forcibly terminated");
    assertThat(activeService.activeSlotCount()).isEqualTo(0);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(0);
  }

  @Test
  void releaseSlotOnPauseFreesSlotForQueuedJobAndRequiresSlotReacquisitionOnResume() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-release-on-pause").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(3);
    props.setMaxActiveJobsPerUser(5);
    props.setReleaseSlotOnPause(true);

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

    TrainingStartResponse aliceJob = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 6), "alice");
    TrainingStartResponse bobJob = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "bob");
    assertThat(aliceJob.status()).isEqualTo("running");
    assertThat(bobJob.status()).isEqualTo("queued");

    FakeWorkerProcess aliceWorker = launcher.awaitProcess(aliceJob.jobId(), Duration.ofSeconds(2));
    activeService.pause("alice", aliceJob.jobId());
    aliceWorker.emitControl("paused", "Paused on batch boundary.");

    // Once aliceWorker confirms paused, its slot is released and bobJob automatically promotes from queued to running!
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "paused".equals(activeService.status("alice", aliceJob.jobId()).status())
            && "running".equals(activeService.status("bob", bobJob.jobId()).status()),
        "Expected paused confirmation to release slot and promote bob's queued job"
    );
    assertThat(activeService.activeSlotCount()).isEqualTo(1);
    FakeWorkerProcess bobWorker = launcher.awaitProcess(bobJob.jobId(), Duration.ofSeconds(2));

    // Attempting to resume aliceJob while bobJob occupies the single slot returns HTTP 429 TRAINING_CAPACITY_EXCEEDED
    assertThatThrownBy(() -> activeService.resume("alice", aliceJob.jobId()))
        .isInstanceOf(TrainingCapacityException.class)
        .satisfies(ex -> {
          TrainingCapacityException capEx = (TrainingCapacityException) ex;
          assertThat(capEx.httpStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
          assertThat(capEx.code()).isEqualTo("TRAINING_CAPACITY_EXCEEDED");
          assertThat(capEx.details()).containsEntry("reason", "resume_slot_unavailable");
        });
    assertThat(activeService.status("alice", aliceJob.jobId()).status()).isEqualTo("paused");
    assertThat(activeService.status("alice", aliceJob.jobId()).pendingControl()).isNull();

    // After bobJob completes and frees the slot, resuming aliceJob re-acquires the slot and finishes cleanly
    bobWorker.completeCleanly();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(activeService.status("bob", bobJob.jobId()).status())
            && activeService.activeSlotCount() == 0,
        "Expected bobJob to complete and free slot"
    );

    TrainingControlResponse resumeAccepted = activeService.resume("alice", aliceJob.jobId());
    assertThat(resumeAccepted.accepted()).isTrue();
    assertThat(resumeAccepted.pendingControl()).isEqualTo("resume");
    assertThat(activeService.activeSlotCount()).isEqualTo(1);

    aliceWorker.emitControl("running", "Resumed.");
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "running".equals(activeService.status("alice", aliceJob.jobId()).status()),
        "Expected aliceJob to transition back to running"
    );
    aliceWorker.completeCleanly();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(activeService.status("alice", aliceJob.jobId()).status())
            && activeService.activeSlotCount() == 0
            && activeService.activeManagedProcessCount() == 0,
        "Expected aliceJob to complete and release slot"
    );
  }

  @Test
  void workerControlStoppedRecordsRealProcessExitCodeAndEvictsTerminalRuntimeAfterRetention() throws Exception {
    userRepository.saveAndFlush(new AppUser("alice", "hash-a", "Alice"));

    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-exit-and-evict").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(4);
    props.setTerminalRuntimeRetention(Duration.ofMillis(75));

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    activeService = new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        eventRepository,
        (req, ds) -> {},
        launcher,
        props,
        new ObjectMapper(),
        tempDir.resolve("datasets").toString()
    );

    TrainingStartResponse job = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "alice");
    FakeWorkerProcess worker = launcher.awaitProcess(job.jobId(), Duration.ofSeconds(2));
    assertThat(activeService.runtimeCountForTest()).isEqualTo(1);

    // Write checkpoint file and emit test_result so auto-checkpoint persists in H2 before stop
    Files.createDirectories(worker.request().checkpointFile().getParent());
    Files.writeString(worker.request().checkpointFile(), "mock-weights", StandardCharsets.UTF_8);
    worker.emitJson(Map.of(
        "type", "test_result",
        "jobId", job.jobId(),
        "testLoss", 0.25,
        "testAccuracy", 0.91,
        "sampleCount", 18,
        "samples", List.of()
    ));

    activeService.stop("alice", job.jobId());
    // Worker emits 'stopped' control event, then exits with non-zero OS exit code 2
    worker.emitControl("stopped", "Stopped on user request.");
    worker.finish(2);

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "stopped".equals(activeService.status("alice", job.jobId()).status())
            && Integer.valueOf(2).equals(activeService.status("alice", job.jobId()).exitCode()),
        "Expected real OS process exit code 2 to be recorded after worker exit"
    );
    assertThat(activeService.status("alice", job.jobId()).exitCode()).isEqualTo(2);

    // Wait for terminalRuntimeRetention (75ms) to evict ManagedJobRuntime from memory
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> activeService.runtimeCountForTest() == 0,
        "Expected terminal ManagedJobRuntime to be evicted after retention delay"
    );

    // Queries and idempotent save continue working seamlessly from H2 after memory eviction
    TrainingStatusResponse dbStatus = activeService.status("alice", job.jobId());
    assertThat(dbStatus.status()).isEqualTo("stopped");
    assertThat(dbStatus.exitCode()).isEqualTo(2);

    TrainingEventsResponse dbEvents = activeService.events("alice", job.jobId(), 0L, 20);
    assertThat(dbEvents.events()).hasSize(2);

    TrainingControlResponse savedAfterEviction = activeService.save("alice", job.jobId());
    assertThat(savedAfterEviction.accepted()).isTrue();
    assertThat(savedAfterEviction.message()).contains("Checkpoint saved:");
  }
}
