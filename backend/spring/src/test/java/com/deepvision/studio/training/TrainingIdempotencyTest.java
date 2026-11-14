package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.TrainingDtos.StartTrainingRequest;
import com.deepvision.studio.training.TrainingDtos.TrainingStartResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
class TrainingIdempotencyTest {
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
  void duplicateSubmissionWithSameIdempotencyKeyCreatesOnlyOneJobAndAtMostOneProcess() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-idem").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxActiveJobsPerUser(1);
    props.setMaxQueueSize(0);

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

    StartTrainingRequest request = TrainingTestFixtures.sampleStartRequest(32, 10);

    // 8 concurrent threads submitting the exact same Idempotency-Key for the same user
    int threadCount = 8;
    ExecutorService pool = Executors.newFixedThreadPool(threadCount);
    CountDownLatch ready = new CountDownLatch(threadCount);
    CountDownLatch go = new CountDownLatch(1);
    List<Future<TrainingStartResponse>> futures = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      futures.add(pool.submit(() -> {
        ready.countDown();
        assertThat(go.await(5, TimeUnit.SECONDS)).isTrue();
        return activeService.start(request, "alice", "idem-key-001");
      }));
    }
    assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
    go.countDown();

    List<TrainingStartResponse> responses = new ArrayList<>();
    for (Future<TrainingStartResponse> future : futures) {
      responses.add(future.get(5, TimeUnit.SECONDS));
    }
    pool.shutdownNow();

    Set<String> uniqueJobIds = responses.stream().map(TrainingStartResponse::jobId).collect(Collectors.toSet());
    assertThat(uniqueJobIds).hasSize(1);
    assertThat(jobRepository.count()).isEqualTo(1);
    assertThat(launcher.totalLaunches()).isEqualTo(1);
    assertThat(launcher.liveProcessCount()).isEqualTo(1);
    assertThat(responses.stream().filter(TrainingStartResponse::idempotentReplay).count()).isEqualTo(threadCount - 1);

    // Sequential retry also returns the exact same job even when user active limit (1) and queue (0) are saturated
    TrainingStartResponse sequentialRetry = activeService.start(request, "alice", "  idem-key-001  ");
    assertThat(sequentialRetry.jobId()).isEqualTo(uniqueJobIds.iterator().next());
    assertThat(sequentialRetry.idempotentReplay()).isTrue();
    assertThat(jobRepository.count()).isEqualTo(1);
    assertThat(launcher.totalLaunches()).isEqualTo(1);
  }

  @Test
  void reusingIdempotencyKeyWithDifferentPayloadThrowsConflictAndIsolatesAcrossUsers() {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-idem-conflict").toString());
    props.setMaxConcurrentJobs(2);
    props.setMaxActiveJobsPerUser(4);
    props.setMaxQueueSize(2);

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

    StartTrainingRequest reqA = TrainingTestFixtures.sampleStartRequest(32, 10);
    StartTrainingRequest reqB = TrainingTestFixtures.sampleStartRequest(64, 10);

    TrainingStartResponse aliceFirst = activeService.start(reqA, "alice", "shared-client-key");
    assertThat(aliceFirst.idempotentReplay()).isFalse();

    assertThatThrownBy(() -> activeService.start(reqB, "alice", "shared-client-key"))
        .isInstanceOf(TrainingStateConflictException.class)
        .satisfies(ex -> {
          TrainingStateConflictException conflict = (TrainingStateConflictException) ex;
          assertThat(conflict.httpStatus()).isEqualTo(HttpStatus.CONFLICT);
          assertThat(conflict.code()).isEqualTo("IDEMPOTENCY_KEY_CONFLICT");
        });

    // Different user can independently use the same Idempotency-Key value
    TrainingStartResponse bobFirst = activeService.start(reqB, "bob", "shared-client-key");
    assertThat(bobFirst.jobId()).isNotEqualTo(aliceFirst.jobId());
    assertThat(jobRepository.count()).isEqualTo(2);
    assertThat(launcher.totalLaunches()).isEqualTo(2);
  }

  @Test
  void idempotentReplaySucceedsBeforeDatasetLookupEvenAfterDatasetDeleted() {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-idem-deleted-ds").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxActiveJobsPerUser(2);
    props.setMaxQueueSize(2);

    AtomicBoolean datasetDeleted = new AtomicBoolean(false);
    TrainingDatasetService datasetService = mock(TrainingDatasetService.class);
    when(datasetService.getDetail(anyString(), anyString())).thenAnswer(invocation -> {
      if (datasetDeleted.get()) {
        throw new IllegalArgumentException("Dataset not found: " + invocation.getArgument(0));
      }
      return TrainingTestFixtures.sampleDataset(invocation.getArgument(0));
    });

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    activeService = new TrainingJobService(
        datasetService,
        userRepository,
        checkpointRepository,
        jobRepository,
        (req, ds) -> {},
        launcher,
        props,
        new ObjectMapper(),
        tempDir.resolve("datasets").toString()
    );

    StartTrainingRequest req = TrainingTestFixtures.sampleStartRequest("custom-ds-1", 32, 10);
    TrainingStartResponse first = activeService.start(req, "alice", "idem-after-delete");
    assertThat(first.idempotentReplay()).isFalse();

    // Simulate dataset deletion before client retries the same Idempotency-Key
    datasetDeleted.set(true);

    TrainingStartResponse replay = activeService.start(req, "alice", "idem-after-delete");
    assertThat(replay.jobId()).isEqualTo(first.jobId());
    assertThat(replay.idempotentReplay()).isTrue();
    assertThat(launcher.totalLaunches()).isEqualTo(1);

    // Without Idempotency-Key (new submission), deleted dataset still fails validation as expected
    assertThatThrownBy(() -> activeService.start(req, "alice", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Dataset not found");
  }
}
