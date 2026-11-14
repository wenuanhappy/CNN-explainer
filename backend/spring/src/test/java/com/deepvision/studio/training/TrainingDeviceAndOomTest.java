package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;

import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.ControllableTrainingProcessLauncher.FakeWorkerProcess;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@DataJpaTest
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class TrainingDeviceAndOomTest {
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
  void persistsReportedWorkerDeviceAndPropagatesCudaOomFriendlyReasonAndExitCode() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-device-oom").toString());
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

    TrainingStartResponse started = activeService.start(TrainingTestFixtures.sampleStartRequest(128, 10), "alice");
    FakeWorkerProcess worker = launcher.awaitProcess(started.jobId(), Duration.ofSeconds(2));

    worker.emitControlWithDevice("running", "cuda", "Training started on cuda.");
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "cuda".equals(activeService.status("alice", started.jobId()).device()),
        "Expected status to reflect reported 'cuda' device"
    );
    assertThat(jobRepository.findById(started.jobId()).orElseThrow().getDevice()).isEqualTo("cuda");

    // Simulate CUDA OOM failure emitted by training_worker.py (exit code 2)
    worker.emitError(
        "CUDA_OOM",
        "GPU 显存不足（CUDA Out of Memory）：建议减小 batchSize（批大小）、缩小输入图像尺寸或减少网络通道数后重试。"
    );
    worker.finish(2);

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "failed".equals(activeService.status("alice", started.jobId()).status()),
        "Expected CUDA OOM job to transition to failed"
    );

    TrainingStatusResponse failed = activeService.status("alice", started.jobId());
    assertThat(failed.device()).isEqualTo("cuda");
    assertThat(failed.exitCode()).isEqualTo(2);
    assertThat(failed.errorCode()).isEqualTo("CUDA_OOM");
    assertThat(failed.errorReason()).contains("batchSize");
    assertThat(activeService.activeSlotCount()).isEqualTo(0);
  }
}
