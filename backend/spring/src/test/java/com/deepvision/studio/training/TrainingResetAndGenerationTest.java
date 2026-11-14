package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.deepvision.studio.auth.AppUser;
import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.ControllableTrainingProcessLauncher.FakeWorkerProcess;
import com.deepvision.studio.training.TrainingDtos.TrainingControlResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingStartResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
class TrainingResetAndGenerationTest {
  @Autowired
  private TrainingJobRepository jobRepository;

  @Autowired
  private AppUserRepository userRepository;

  @Autowired
  private TrainingCheckpointRepository checkpointRepository;

  @TempDir
  Path tempDir;

  private TrainingJobService activeService;

  @BeforeEach
  void setUpUsers() {
    userRepository.saveAndFlush(new AppUser("alice", "hash-a", "Alice"));
    userRepository.saveAndFlush(new AppUser("bob", "hash-b", "Bob"));
  }

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
  void resetRejectsNonTerminalJobsWith409AndStopThenResetIgnoresLateOldProcessExit() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-reset-race").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(1);
    props.setStopTimeout(Duration.ofMillis(80));

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

    TrainingStartResponse job1 = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 6), "alice");
    FakeWorkerProcess worker1 = launcher.awaitProcess(job1.jobId(), Duration.ofSeconds(2));
    worker1.setDelayExitOnDestroy(true);

    // 1. Calling reset on a running job must return HTTP 409 INVALID_STATE_TRANSITION
    assertThatThrownBy(() -> activeService.reset("alice", job1.jobId()))
        .isInstanceOf(TrainingStateConflictException.class)
        .satisfies(ex -> {
          TrainingStateConflictException conflict = (TrainingStateConflictException) ex;
          assertThat(conflict.httpStatus()).isEqualTo(HttpStatus.CONFLICT);
          assertThat(conflict.code()).isEqualTo("INVALID_STATE_TRANSITION");
        });

    // Calling reset on a queued job also returns HTTP 409
    TrainingStartResponse bobQueued = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "bob");
    assertThat(bobQueued.status()).isEqualTo("queued");
    assertThatThrownBy(() -> activeService.reset("bob", bobQueued.jobId()))
        .isInstanceOf(TrainingStateConflictException.class)
        .satisfies(ex -> assertThat(((TrainingStateConflictException) ex).httpStatus()).isEqualTo(HttpStatus.CONFLICT));
    activeService.stop("bob", bobQueued.jobId());

    // Calling reset on a paused job also returns HTTP 409
    activeService.pause("alice", job1.jobId());
    worker1.emitControl("paused", "Paused.");
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "paused".equals(activeService.status("alice", job1.jobId()).status()),
        "Expected job1 to become paused"
    );
    assertThatThrownBy(() -> activeService.reset("alice", job1.jobId()))
        .isInstanceOf(TrainingStateConflictException.class)
        .satisfies(ex -> assertThat(((TrainingStateConflictException) ex).httpStatus()).isEqualTo(HttpStatus.CONFLICT));

    // 2. Stop job1; stopTimeout fires while worker1's OS exit is still delayed in kernel teardown
    activeService.stop("alice", job1.jobId());
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "stopped".equals(activeService.status("alice", job1.jobId()).status()),
        "Expected job1 to transition to stopped on stopTimeout"
    );
    assertThat(activeService.activeSlotCount()).isEqualTo(0);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(0);

    // 3. Reset stopped job1 -> creates new job2 via start(), occupying the 1 slot
    TrainingControlResponse resetResp = activeService.reset("alice", job1.jobId());
    String job2Id = resetResp.jobId();
    assertThat(job2Id).isNotEqualTo(job1.jobId());
    assertThat(resetResp.status()).isEqualTo("running");
    assertThat(activeService.activeSlotCount()).isEqualTo(1);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(1);

    FakeWorkerProcess worker2 = launcher.awaitProcess(job2Id, Duration.ofSeconds(2));
    assertThat(worker2).isNotSameAs(worker1);

    // Resetting job1 again while alice already has 1 active job (maxActiveJobsPerUser = 1) throws HTTP 429
    assertThatThrownBy(() -> activeService.reset("alice", job1.jobId()))
        .isInstanceOf(TrainingCapacityException.class)
        .satisfies(ex -> assertThat(((TrainingCapacityException) ex).code()).isEqualTo("USER_ACTIVE_JOB_LIMIT_EXCEEDED"));

    // 4. Late exit and trailing output from old worker1 must NOT fail job2 or release job2's slot
    worker1.emitTrailingErrorBeforeDelayedExit("WORKER_RUNTIME_ERROR", "Late stderr from old process");
    worker1.completeDelayedExit(137);
    Thread.sleep(120);

    assertThat(activeService.status("alice", job1.jobId()).status()).isEqualTo("stopped");
    assertThat(activeService.status("alice", job2Id).status()).isEqualTo("running");
    assertThat(activeService.activeSlotCount()).isEqualTo(1);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(1);

    // Bob starts a job -> must enter queue because job2 still holds the single execution slot
    TrainingStartResponse bobCheck = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 2), "bob");
    assertThat(bobCheck.status()).isEqualTo("queued");
    assertThat(activeService.activeSlotCount()).isEqualTo(1);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(1);

    worker2.completeCleanly();
    FakeWorkerProcess bobWorker = launcher.awaitProcess(bobCheck.jobId(), Duration.ofSeconds(2));
    bobWorker.completeCleanly();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> activeService.activeSlotCount() == 0 && activeService.activeManagedProcessCount() == 0,
        "Expected all slots and processes to finish cleanly"
    );
  }

  @Test
  void staleRunGenerationCallbacksOnSameRuntimeNeverFailNewRunOrReleaseItsSlot() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-gen-guard").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(4);

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
    FakeWorkerProcess gen1Worker = launcher.awaitProcess(started.jobId(), Duration.ofSeconds(2));
    gen1Worker.setDelayExitOnDestroy(true);

    // Supersede generation 1 with generation 2 on the exact same runtime while gen1Worker exit is still pending
    activeService.relaunchWithNewGenerationForTest(started.jobId());
    assertThat(launcher.allProcesses()).hasSize(2);
    FakeWorkerProcess gen2Worker = launcher.allProcesses().get(1);
    assertThat(gen2Worker).isNotSameAs(gen1Worker);
    assertThat(activeService.activeSlotCount()).isEqualTo(1);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(1);

    // Generation-1 worker emits trailing error and exits with non-zero code 137 AFTER generation 2 is running
    gen1Worker.emitTrailingErrorBeforeDelayedExit("WORKER_RUNTIME_ERROR", "Stale generation crash");
    gen1Worker.completeDelayedExit(137);
    Thread.sleep(120);

    // Generation-2 run must remain running with its slot intact
    assertThat(activeService.status("alice", started.jobId()).status()).isEqualTo("running");
    assertThat(activeService.status("alice", started.jobId()).errorCode()).isNull();
    assertThat(activeService.activeSlotCount()).isEqualTo(1);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(1);

    // Submitting another job must queue rather than breaching maxConcurrentJobs = 1
    TrainingStartResponse queued = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 2), "bob");
    assertThat(queued.status()).isEqualTo("queued");
    assertThat(activeService.activeSlotCount()).isEqualTo(1);
    assertThat(activeService.activeManagedProcessCount()).isEqualTo(1);

    gen2Worker.completeCleanly();
    launcher.awaitProcess(queued.jobId(), Duration.ofSeconds(2)).completeCleanly();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> activeService.activeSlotCount() == 0 && activeService.activeManagedProcessCount() == 0,
        "Expected all slots and managed processes to drain cleanly"
    );
  }

  @Test
  void resetOnCompletedJobCreatesNewJobIdAndPreservesOriginalCheckpointFile() throws Exception {
    Path jobsRoot = tempDir.resolve("jobs-reset-ckpt");
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(jobsRoot.toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(4);

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

    TrainingStartResponse job1 = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 2), "alice");
    FakeWorkerProcess worker1 = launcher.awaitProcess(job1.jobId(), Duration.ofSeconds(2));
    Path ckpt1 = worker1.request().checkpointFile();
    Files.createDirectories(ckpt1.getParent());
    Files.writeString(ckpt1, "checkpoint-bytes-original-job1", StandardCharsets.UTF_8);
    worker1.emitJson(Map.of(
        "type", "test_result",
        "jobId", job1.jobId(),
        "testLoss", 0.15,
        "testAccuracy", 0.94,
        "sampleCount", 20
    ));
    worker1.completeCleanly();

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(activeService.status("alice", job1.jobId()).status()),
        "Expected job1 to complete"
    );

    TrainingControlResponse resetResp = activeService.reset("alice", job1.jobId());
    String job2Id = resetResp.jobId();
    assertThat(job2Id).isNotEqualTo(job1.jobId());
    assertThat(resetResp.status()).isEqualTo("running");

    FakeWorkerProcess worker2 = launcher.awaitProcess(job2Id, Duration.ofSeconds(2));
    Path ckpt2 = worker2.request().checkpointFile();
    assertThat(ckpt2).isNotEqualTo(ckpt1);
    Files.writeString(ckpt2, "checkpoint-bytes-new-job2", StandardCharsets.UTF_8);
    worker2.completeCleanly();

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(activeService.status("alice", job2Id).status()),
        "Expected job2 to complete"
    );

    assertThat(Files.readString(ckpt1, StandardCharsets.UTF_8)).isEqualTo("checkpoint-bytes-original-job1");
    assertThat(Files.readString(ckpt2, StandardCharsets.UTF_8)).isEqualTo("checkpoint-bytes-new-job2");
    assertThat(activeService.status("alice", job1.jobId()).status()).isEqualTo("completed");
  }
}
