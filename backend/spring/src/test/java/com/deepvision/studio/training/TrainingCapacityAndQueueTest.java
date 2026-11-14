package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.ControllableTrainingProcessLauncher.FakeWorkerProcess;
import com.deepvision.studio.training.TrainingDtos.TrainingStartResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
class TrainingCapacityAndQueueTest {
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
  void enforcesConcurrencyTwoQueueThreeOnSixSimultaneousStartsAndPromotesQueuedJobs() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs").toString());
    props.setMaxConcurrentJobs(2);
    props.setMaxQueueSize(3);
    props.setMaxActiveJobsPerUser(10);
    props.setJobTimeout(Duration.ofSeconds(30));

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

    ExecutorService callerPool = Executors.newFixedThreadPool(6);
    CountDownLatch ready = new CountDownLatch(6);
    CountDownLatch startGate = new CountDownLatch(1);
    List<Callable<Object>> tasks = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      final int idx = i;
      tasks.add(() -> {
        ready.countDown();
        assertThat(startGate.await(5, TimeUnit.SECONDS)).isTrue();
        try {
          return activeService.start(TrainingTestFixtures.sampleStartRequest(32, 5 + idx), "user-" + idx);
        } catch (TrainingCapacityException ex) {
          return ex;
        }
      });
    }

    List<Future<Object>> futures = new ArrayList<>();
    for (Callable<Object> task : tasks) {
      futures.add(callerPool.submit(task));
    }
    assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
    startGate.countDown();

    List<TrainingStartResponse> accepted = new ArrayList<>();
    List<TrainingCapacityException> rejected = new ArrayList<>();
    for (Future<Object> future : futures) {
      try {
        Object result = future.get(5, TimeUnit.SECONDS);
        if (result instanceof TrainingStartResponse resp) {
          accepted.add(resp);
        } else if (result instanceof TrainingCapacityException capEx) {
          rejected.add(capEx);
        } else {
          throw new AssertionError("Unexpected result: " + result);
        }
      } catch (ExecutionException ex) {
        throw new AssertionError("Unexpected exception during concurrent start", ex.getCause());
      }
    }
    callerPool.shutdownNow();

    assertThat(accepted).hasSize(5);
    assertThat(rejected).hasSize(1);

    TrainingCapacityException overflow = rejected.get(0);
    assertThat(overflow.httpStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    assertThat(overflow.code()).isEqualTo("TRAINING_CAPACITY_EXCEEDED");

    List<TrainingStartResponse> runningResponses = accepted.stream()
        .filter(r -> "running".equals(r.status()))
        .toList();
    List<TrainingStartResponse> queuedResponses = accepted.stream()
        .filter(r -> "queued".equals(r.status()))
        .toList();

    assertThat(runningResponses).hasSize(2);
    assertThat(queuedResponses).hasSize(3);
    assertThat(activeService.activeSlotCount()).isEqualTo(2);
    assertThat(activeService.queuedJobCount()).isEqualTo(3);
    assertThat(launcher.totalLaunches()).isEqualTo(2);
    assertThat(launcher.liveProcessCount()).isEqualTo(2);
    assertThat(jobRepository.count()).isEqualTo(5);

    // Finish the first running worker and verify a queued job automatically starts
    FakeWorkerProcess firstWorker = launcher.awaitProcess(runningResponses.get(0).jobId(), Duration.ofSeconds(2));
    firstWorker.completeCleanly();

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(3),
        () -> launcher.totalLaunches() == 3 && activeService.queuedJobCount() == 2,
        "Expected first queued job to be promoted after slot release"
    );
    assertThat(activeService.activeSlotCount()).isEqualTo(2);
  }

  @Test
  void enforcesPerUserActiveJobLimit() {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-user-limit").toString());
    props.setMaxConcurrentJobs(2);
    props.setMaxQueueSize(4);
    props.setMaxActiveJobsPerUser(2);

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

    activeService.start(TrainingTestFixtures.sampleStartRequest(32, 4), "alice");
    activeService.start(TrainingTestFixtures.sampleStartRequest(32, 6), "alice");

    assertThatThrownBy(() -> activeService.start(TrainingTestFixtures.sampleStartRequest(32, 8), "alice"))
        .isInstanceOf(TrainingCapacityException.class)
        .satisfies(ex -> {
          TrainingCapacityException cap = (TrainingCapacityException) ex;
          assertThat(cap.httpStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
          assertThat(cap.code()).isEqualTo("USER_ACTIVE_JOB_LIMIT_EXCEEDED");
        });

    // Another user can still submit into queue while alice is at her per-user cap
    TrainingStartResponse bobResp = activeService.start(TrainingTestFixtures.sampleStartRequest(32, 8), "bob");
    assertThat(bobResp.status()).isEqualTo("queued");
  }
}
