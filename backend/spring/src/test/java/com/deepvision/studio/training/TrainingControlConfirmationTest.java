package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.ControllableTrainingProcessLauncher.FakeWorkerProcess;
import com.deepvision.studio.training.TrainingDtos.TrainingControlResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingStartResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingStatusResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
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
}
