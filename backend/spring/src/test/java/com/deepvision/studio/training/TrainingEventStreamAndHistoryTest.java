package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.deepvision.studio.auth.AppUser;
import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.training.ControllableTrainingProcessLauncher.FakeWorkerProcess;
import com.deepvision.studio.training.TrainingDtos.TrainingEventsResponse;
import com.deepvision.studio.training.TrainingDtos.TrainingStartResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

@DataJpaTest
@ActiveProfiles("test")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class TrainingEventStreamAndHistoryTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired
  private TrainingJobRepository jobRepository;

  @Autowired
  private TrainingJobEventRepository eventRepository;

  @Autowired
  private AppUserRepository userRepository;

  @Autowired
  private TrainingCheckpointRepository checkpointRepository;

  @TempDir
  Path tempDir;

  private final List<TrainingJobService> servicesToClose = new ArrayList<>();

  @BeforeEach
  void setUpUsers() {
    userRepository.saveAndFlush(new AppUser("alice", "hash-a", "Alice"));
    userRepository.saveAndFlush(new AppUser("bob", "hash-b", "Bob"));
  }

  @AfterEach
  void tearDown() {
    for (TrainingJobService svc : servicesToClose) {
      svc.shutdown();
    }
    servicesToClose.clear();
    eventRepository.deleteAll();
    checkpointRepository.deleteAll();
    jobRepository.deleteAll();
    userRepository.deleteAll();
  }

  @Test
  void stampsEnvelopeOnAllStreamEventsAndSupportsAfterSequenceIncrementalQuery() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-events").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(4);
    props.setMaxHistoryEvents(50);

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    TrainingJobService service = registerService(new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        eventRepository,
        (req, ds) -> {},
        launcher,
        props,
        MAPPER,
        tempDir.resolve("datasets").toString()
    ));

    TrainingStartResponse started = service.start(TrainingTestFixtures.sampleStartRequest(16, 2), "alice");
    FakeWorkerProcess worker = launcher.awaitProcess(started.jobId(), Duration.ofSeconds(2));

    List<String> wsMessages = new CopyOnWriteArrayList<>();
    WebSocketSession wsSession = mockOpenSession(wsMessages);
    service.addSession("alice", started.jobId(), wsSession);

    worker.emitControlWithDevice("running", "cpu", "Training started on cpu.");
    worker.emitJson(sampleMetricPayload(started.jobId(), 1, 1, 0.42, 0.85));
    worker.emitJson(Map.of(
        "type", "backprop",
        "jobId", started.jobId(),
        "phase", "backward",
        "epoch", 1
    ));
    worker.completeCleanly();

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(service.status("alice", started.jobId()).status()) && wsMessages.size() >= 4,
        "Expected job completion and 4 streamed events"
    );

    assertThat(service.status("alice", started.jobId()).lastSequence()).isEqualTo(4L);
    assertThat(wsMessages).hasSize(4);

    for (int i = 0; i < wsMessages.size(); i++) {
      JsonNode event = MAPPER.readTree(wsMessages.get(i));
      assertThat(event.get("jobId").asText()).isEqualTo(started.jobId());
      assertThat(event.get("sequence").asLong()).isEqualTo(i + 1L);
      assertThat(event.get("schemaVersion").asText()).isEqualTo(TrainingJobService.EVENT_SCHEMA_VERSION);
      assertThat(Instant.parse(event.get("timestamp").asText())).isNotNull();
    }

    TrainingEventsResponse full = service.events("alice", started.jobId(), 0L, 100);
    assertThat(full.jobId()).isEqualTo(started.jobId());
    assertThat(full.status()).isEqualTo("completed");
    assertThat(full.schemaVersion()).isEqualTo("1.0");
    assertThat(full.lastSequence()).isEqualTo(4L);
    assertThat(full.earliestAvailableSequence()).isEqualTo(1L);
    assertThat(full.truncated()).isFalse();
    assertThat(full.events()).hasSize(4);
    assertThat(full.events().get(0).get("type").asText()).isEqualTo("control");
    assertThat(full.events().get(0).get("status").asText()).isEqualTo("running");
    assertThat(full.events().get(1).get("type").asText()).isEqualTo("metric");
    assertThat(full.events().get(1).get("loss").asDouble()).isEqualTo(0.42);
    assertThat(full.events().get(2).get("type").asText()).isEqualTo("backprop");
    assertThat(full.events().get(2).get("phase").asText()).isEqualTo("backward");
    assertThat(full.events().get(3).get("type").asText()).isEqualTo("control");
    assertThat(full.events().get(3).get("status").asText()).isEqualTo("completed");

    TrainingEventsResponse incremental = service.events("alice", started.jobId(), 2L, 100);
    assertThat(incremental.truncated()).isFalse();
    assertThat(incremental.events()).extracting(e -> e.get("sequence").asLong()).containsExactly(3L, 4L);

    assertThatThrownBy(() -> service.events("bob", started.jobId(), 0L, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not found");
    assertThatThrownBy(() -> service.events("alice", started.jobId(), -1L, 10))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> service.events("alice", started.jobId(), 0L, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void retainsBoundedHistoryAndReportsTruncationAndPersistsAcrossServiceRestart() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-bounded").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(4);
    props.setMaxHistoryEvents(5);

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    TrainingJobService service1 = registerService(new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        eventRepository,
        (req, ds) -> {},
        launcher,
        props,
        MAPPER,
        tempDir.resolve("datasets").toString()
    ));

    TrainingStartResponse started = service1.start(TrainingTestFixtures.sampleStartRequest(16, 8), "alice");
    FakeWorkerProcess worker = launcher.awaitProcess(started.jobId(), Duration.ofSeconds(2));

    worker.emitControl("running", "Started.");
    for (int step = 1; step <= 7; step++) {
      worker.emitJson(sampleMetricPayload(started.jobId(), step, step, 1.0 / step, 0.5 + step * 0.05));
    }
    worker.completeCleanly();

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(service1.status("alice", started.jobId()).status()),
        "Expected bounded history job to complete"
    );

    // Total emitted = 9 events (1 running + 7 metric + 1 completed); retention cap = 5 -> sequences 5..9 kept
    TrainingEventsResponse fromZero = service1.events("alice", started.jobId(), 0L, 100);
    assertThat(fromZero.lastSequence()).isEqualTo(9L);
    assertThat(fromZero.earliestAvailableSequence()).isEqualTo(5L);
    assertThat(fromZero.truncated()).isTrue();
    assertThat(fromZero.events()).extracting(e -> e.get("sequence").asLong())
        .containsExactly(5L, 6L, 7L, 8L, 9L);

    TrainingEventsResponse fromFour = service1.events("alice", started.jobId(), 4L, 100);
    assertThat(fromFour.truncated()).isFalse();
    assertThat(fromFour.events()).extracting(e -> e.get("sequence").asLong())
        .containsExactly(5L, 6L, 7L, 8L, 9L);

    // Seed an orphaned active job in DB and simulate service restart
    Instant now = Instant.now();
    TrainingJob orphan = new TrainingJob(
        "job-orphan-restart",
        "alice",
        "builtin-moons",
        "digest-orphan",
        "sig-orphan",
        TrainingJobStatus.RUNNING.wireValue(),
        null,
        5,
        10,
        "{}",
        now
    );
    jobRepository.saveAndFlush(orphan);
    service1.shutdown();

    TrainingJobService service2 = registerService(new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        eventRepository,
        (req, ds) -> {},
        new ControllableTrainingProcessLauncher(),
        props,
        MAPPER,
        tempDir.resolve("datasets").toString()
    ));

    // Historical events for completed job remain queryable from H2 after restart
    TrainingEventsResponse afterRestart = service2.events("alice", started.jobId(), 6L, 10);
    assertThat(afterRestart.truncated()).isFalse();
    assertThat(afterRestart.events()).extracting(e -> e.get("sequence").asLong())
        .containsExactly(7L, 8L, 9L);

    // Orphaned job recovered on startup also emits a sequenced 'interrupted' event
    TrainingEventsResponse orphanEvents = service2.events("alice", "job-orphan-restart", 0L, 10);
    assertThat(orphanEvents.status()).isEqualTo("interrupted");
    assertThat(orphanEvents.lastSequence()).isEqualTo(1L);
    assertThat(orphanEvents.events()).hasSize(1);
    JsonNode recoveryEvent = orphanEvents.events().get(0);
    assertThat(recoveryEvent.get("jobId").asText()).isEqualTo("job-orphan-restart");
    assertThat(recoveryEvent.get("sequence").asLong()).isEqualTo(1L);
    assertThat(recoveryEvent.get("schemaVersion").asText()).isEqualTo("1.0");
    assertThat(recoveryEvent.get("status").asText()).isEqualTo("interrupted");
    assertThat(recoveryEvent.get("errorCode").asText()).isEqualTo("SERVICE_RESTART_INTERRUPTED");
  }

  @Test
  void prunesEventHistoryInBatchesDuringStreamingWhileKeepingExactWindowOnRead() throws Exception {
    TrainingProperties props = new TrainingProperties();
    props.setJobsRoot(tempDir.resolve("jobs-batch-prune").toString());
    props.setMaxConcurrentJobs(1);
    props.setMaxQueueSize(2);
    props.setMaxActiveJobsPerUser(4);
    props.setMaxHistoryEvents(10);

    ControllableTrainingProcessLauncher launcher = new ControllableTrainingProcessLauncher();
    TrainingJobService service = registerService(new TrainingJobService(
        TrainingTestFixtures.mockDatasetService(),
        userRepository,
        checkpointRepository,
        jobRepository,
        eventRepository,
        (req, ds) -> {},
        launcher,
        props,
        MAPPER,
        tempDir.resolve("datasets").toString()
    ));

    TrainingStartResponse started = service.start(TrainingTestFixtures.sampleStartRequest(16, 30), "alice");
    FakeWorkerProcess worker = launcher.awaitProcess(started.jobId(), Duration.ofSeconds(2));

    // Emit 1 control + 14 metric events = 15 events; overflow is 5 (< EVENT_PRUNE_BATCH_SIZE=16) so DB has not pruned yet
    worker.emitControl("running", "Started.");
    for (int step = 1; step <= 14; step++) {
      worker.emitJson(sampleMetricPayload(started.jobId(), step, step, 1.0 / step, 0.50));
    }

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> eventRepository.count() == 15L,
        "Expected 15 persisted events without per-event pruning"
    );
    assertThat(eventRepository.count()).isEqualTo(15L);

    // Emit 12 more metric events (sequences 16..27); sequence 26 crosses overflow=16 and triggers batch prune to 10 rows,
    // then sequence 27 appends 1 row -> 11 rows stored in H2
    for (int step = 15; step <= 26; step++) {
      worker.emitJson(sampleMetricPayload(started.jobId(), step, step, 1.0 / step, 0.70));
    }

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> service.status("alice", started.jobId()).lastSequence() == 27L && eventRepository.count() == 11L,
        "Expected batch prune to trim stored history once overflow reached batch threshold"
    );
    assertThat(eventRepository.count()).isEqualTo(11L);

    // Reading events flushes dirty entity and trims exact maxHistoryEvents=10 window (sequences 18..27)
    TrainingEventsResponse window = service.events("alice", started.jobId(), 0L, 100);
    assertThat(window.lastSequence()).isEqualTo(27L);
    assertThat(window.earliestAvailableSequence()).isEqualTo(18L);
    assertThat(window.truncated()).isTrue();
    assertThat(window.events()).hasSize(10);
    assertThat(window.events().get(0).get("sequence").asLong()).isEqualTo(18L);
    assertThat(window.events().get(9).get("sequence").asLong()).isEqualTo(27L);
    assertThat(jobRepository.findById(started.jobId()).orElseThrow().getLastSequence()).isEqualTo(27L);

    // Completing the job emits sequence 28 and prunes immediately on terminal event
    worker.completeCleanly();
    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(2),
        () -> "completed".equals(service.status("alice", started.jobId()).status())
            && eventRepository.count() == 10L,
        "Expected terminal event to flush final sequence and prune to exact retention cap"
    );
    assertThat(jobRepository.findById(started.jobId()).orElseThrow().getLastSequence()).isEqualTo(28L);
    TrainingEventsResponse finalWindow = service.events("alice", started.jobId(), 0L, 100);
    assertThat(finalWindow.events()).extracting(e -> e.get("sequence").asLong())
        .containsExactly(19L, 20L, 21L, 22L, 23L, 24L, 25L, 26L, 27L, 28L);
  }

  private TrainingJobService registerService(TrainingJobService service) {
    servicesToClose.add(service);
    return service;
  }

  private static WebSocketSession mockOpenSession(List<String> sink) throws Exception {
    WebSocketSession session = mock(WebSocketSession.class);
    when(session.isOpen()).thenReturn(true);
    doAnswer(invocation -> {
      TextMessage msg = invocation.getArgument(0);
      sink.add(msg.getPayload());
      return null;
    }).when(session).sendMessage(any(TextMessage.class));
    return session;
  }

  private static Map<String, Object> sampleMetricPayload(
      String jobId,
      int step,
      int epoch,
      double loss,
      double accuracy
  ) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("type", "metric");
    map.put("jobId", jobId);
    map.put("step", step);
    map.put("epoch", epoch);
    map.put("batch", 1);
    map.put("totalEpochs", 8);
    map.put("totalBatches", 4);
    map.put("loss", loss);
    map.put("valLoss", loss * 1.05);
    map.put("accuracy", accuracy);
    map.put("valAccuracy", accuracy * 0.98);
    map.put("lr", 0.001);
    map.put("elapsedSeconds", step);
    map.put("etaSeconds", Math.max(0, 8 - step));
    map.put("gradientNorm", 0.12);
    map.put("weightMean", 0.01);
    map.put("weightStd", 0.15);
    map.put("gradientStatus", "healthy");
    return map;
  }
}
