package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.deepvision.studio.auth.AppUser;
import com.deepvision.studio.auth.AppUserRepository;
import com.deepvision.studio.auth.JwtService;
import com.deepvision.studio.training.TrainingDtos.StartTrainingRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class TrainingEndToEndIntegrationTest {
  private static final AtomicBoolean REJECT_NETWORK = new AtomicBoolean(false);
  private static Path tempRoot;

  @DynamicPropertySource
  static void configureProperties(DynamicPropertyRegistry registry) throws Exception {
    if (tempRoot == null) {
      tempRoot = Files.createTempDirectory("deepvision-e2e-test-");
    }
    String javaExe = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    String testClassesDir = Path.of(
        FakeWorkerMain.class.getProtectionDomain().getCodeSource().getLocation().toURI()
    ).toString();

    registry.add("deepvision.training.jobs-root", () -> tempRoot.resolve("jobs").toString());
    registry.add("deepvision.datasets.root", () -> tempRoot.resolve("datasets").toString());
    registry.add("deepvision.uploads.root", () -> tempRoot.resolve("uploads").toString());
    registry.add("deepvision.training.max-concurrent-jobs", () -> 1);
    registry.add("deepvision.training.max-queue-size", () -> 1);
    registry.add("deepvision.training.max-active-jobs-per-user", () -> 4);
    registry.add("deepvision.training.stop-timeout", () -> "3s");
    registry.add("deepvision.training.job-timeout", () -> "30s");
    registry.add("deepvision.training.worker-command-prefix[0]", () -> javaExe);
    registry.add("deepvision.training.worker-command-prefix[1]", () -> "-cp");
    registry.add("deepvision.training.worker-command-prefix[2]", () -> testClassesDir);
    registry.add("deepvision.training.worker-command-prefix[3]", FakeWorkerMain.class::getName);
  }

  @TestConfiguration
  static class ValidatorHookTestConfig {
    @Bean
    @Primary
    NetworkValidator switchableNetworkValidator() {
      return (request, dataset) -> {
        if (REJECT_NETWORK.get()) {
          throw new TrainingApiException(
              HttpStatus.UNPROCESSABLE_ENTITY,
              "INVALID_NETWORK_GRAPH",
              "Layer dimensions mismatch between input and output.",
              Map.of("datasetId", dataset.id())
          );
        }
      };
    }
  }

  @Autowired
  private MockMvc mockMvc;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private AppUserRepository userRepository;

  @Autowired
  private JwtService jwtService;

  @Autowired
  private TrainingJobService jobService;

  private String bearerToken;

  @BeforeEach
  void setUpUser() {
    REJECT_NETWORK.set(false);
    AppUser user = userRepository.findByUsername("e2e_alice")
        .orElseGet(() -> userRepository.saveAndFlush(new AppUser("e2e_alice", "hash-e2e", "Alice E2E")));
    bearerToken = "Bearer " + jwtService.issue(user);
  }

  @Test
  void runsRestControlPauseSlotPolicyCompletionSaveCheckpointAndEventHistoryOverRealSubprocess() throws Exception {
    // 1. Start long-enough job with Idempotency-Key -> occupies the single execution slot
    StartTrainingRequest longReq = TrainingTestFixtures.sampleStartRequest("points-2d", 16, 40);
    MvcResult startRes = mockMvc.perform(post("/api/training/start")
            .header(HttpHeaders.AUTHORIZATION, bearerToken)
            .header("Idempotency-Key", "e2e-idem-1")
            .header("X-Request-Id", "req-e2e-start-1")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(longReq)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("running"))
        .andExpect(jsonPath("$.idempotentReplay").value(false))
        .andExpect(jsonPath("$.configDigest").isNotEmpty())
        .andReturn();

    String job1Id = objectMapper.readTree(startRes.getResponse().getContentAsString()).get("jobId").asText();

    // 2. Idempotent replay returns exact same job; mismatched payload with same key returns HTTP 409
    mockMvc.perform(post("/api/training/start")
            .header(HttpHeaders.AUTHORIZATION, bearerToken)
            .header("Idempotency-Key", "e2e-idem-1")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(longReq)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.jobId").value(job1Id))
        .andExpect(jsonPath("$.idempotentReplay").value(true));

    StartTrainingRequest diffBatchReq = TrainingTestFixtures.sampleStartRequest("points-2d", 64, 40);
    mockMvc.perform(post("/api/training/start")
            .header(HttpHeaders.AUTHORIZATION, bearerToken)
            .header("Idempotency-Key", "e2e-idem-1")
            .header("X-Request-Id", "req-conflict-409")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(diffBatchReq)))
        .andExpect(status().isConflict())
        .andExpect(header().string("X-Request-Id", "req-conflict-409"))
        .andExpect(jsonPath("$.code").value("IDEMPOTENCY_KEY_CONFLICT"))
        .andExpect(jsonPath("$.requestId").value("req-conflict-409"))
        .andExpect(jsonPath("$.error").isNotEmpty());

    // 3. Pause job1 -> accepted first, confirmed by real OS subprocess worker; paused job keeps slot
    mockMvc.perform(post("/api/training/{jobId}/pause", job1Id)
            .header(HttpHeaders.AUTHORIZATION, bearerToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.accepted").value(true))
        .andExpect(jsonPath("$.pendingControl").value("pause"));

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(5),
        () -> "paused".equals(jobService.status("e2e_alice", job1Id).status()),
        "Expected OS subprocess worker to confirm paused state"
    );
    assertThat(jobService.activeSlotCount()).isEqualTo(1);

    // 4. While job1 is paused (holding the 1 execution slot), job2 enters the 1-item queue and job3 gets HTTP 429
    StartTrainingRequest queuedReq = TrainingTestFixtures.sampleStartRequest("points-2d", 16, 2);
    MvcResult queuedRes = mockMvc.perform(post("/api/training/start")
            .header(HttpHeaders.AUTHORIZATION, bearerToken)
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(queuedReq)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("queued"))
        .andExpect(jsonPath("$.queuePosition").value(1))
        .andReturn();
    String job2Id = objectMapper.readTree(queuedRes.getResponse().getContentAsString()).get("jobId").asText();

    mockMvc.perform(post("/api/training/start")
            .header(HttpHeaders.AUTHORIZATION, bearerToken)
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(queuedReq)))
        .andExpect(status().isTooManyRequests())
        .andExpect(jsonPath("$.code").value("TRAINING_CAPACITY_EXCEEDED"));

    // 5. Stop job1 -> OS worker confirms stop, releases slot, and queued job2 automatically promotes & completes!
    mockMvc.perform(post("/api/training/{jobId}/stop", job1Id)
            .header(HttpHeaders.AUTHORIZATION, bearerToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.accepted").value(true));

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(8),
        () -> "stopped".equals(jobService.status("e2e_alice", job1Id).status())
            && jobService.status("e2e_alice", job1Id).exitCode() != null
            && "completed".equals(jobService.status("e2e_alice", job2Id).status())
            && jobService.status("e2e_alice", job2Id).exitCode() != null,
        "Expected job1 to stop and promoted job2 to complete end-to-end"
    );

    // 6. Verify Mode B normal training flow: status -> save checkpoint -> list checkpoints -> query sequenced events
    mockMvc.perform(get("/api/training/{jobId}/status", job2Id)
            .header(HttpHeaders.AUTHORIZATION, bearerToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.status").value("completed"))
        .andExpect(jsonPath("$.device").value("cpu"))
        .andExpect(jsonPath("$.exitCode").value(0));

    mockMvc.perform(post("/api/training/{jobId}/save", job2Id)
            .header(HttpHeaders.AUTHORIZATION, bearerToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.accepted").value(true));

    MvcResult ckptRes = mockMvc.perform(get("/api/training/checkpoints")
            .param("datasetId", "points-2d")
            .header(HttpHeaders.AUTHORIZATION, bearerToken))
        .andExpect(status().isOk())
        .andReturn();
    JsonNode ckpts = objectMapper.readTree(ckptRes.getResponse().getContentAsString());
    assertThat(ckpts.isArray()).isTrue();
    assertThat(ckpts).anyMatch(node -> job2Id.equals(node.path("jobId").asText()));

    MvcResult eventsRes = mockMvc.perform(get("/api/training/{jobId}/events", job2Id)
            .param("afterSequence", "0")
            .param("limit", "50")
            .header(HttpHeaders.AUTHORIZATION, bearerToken))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.jobId").value(job2Id))
        .andExpect(jsonPath("$.schemaVersion").value("1.0"))
        .andExpect(jsonPath("$.truncated").value(false))
        .andReturn();
    JsonNode eventsRoot = objectMapper.readTree(eventsRes.getResponse().getContentAsString());
    JsonNode eventsArray = eventsRoot.get("events");
    assertThat(eventsArray.size()).isGreaterThanOrEqualTo(5); // queued + running + 2 metrics + test_result + completed
    long prevSeq = 0;
    for (JsonNode ev : eventsArray) {
      long seq = ev.get("sequence").asLong();
      assertThat(seq).isEqualTo(prevSeq + 1L);
      assertThat(ev.get("jobId").asText()).isEqualTo(job2Id);
      assertThat(ev.get("schemaVersion").asText()).isEqualTo("1.0");
      assertThat(ev.get("timestamp").asText()).isNotBlank();
      prevSeq = seq;
    }

    // 7. Resume on completed job returns HTTP 409; unknown job returns HTTP 400; zero leaked slots/processes
    mockMvc.perform(post("/api/training/{jobId}/resume", job2Id)
            .header(HttpHeaders.AUTHORIZATION, bearerToken))
        .andExpect(status().isConflict())
        .andExpect(jsonPath("$.code").value("INVALID_STATE_TRANSITION"));

    mockMvc.perform(get("/api/training/{jobId}/status", "non-existent-job")
            .header(HttpHeaders.AUTHORIZATION, bearerToken))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value("BAD_REQUEST"));

    TrainingTestFixtures.awaitUntil(
        Duration.ofSeconds(3),
        () -> jobService.activeSlotCount() == 0 && jobService.activeManagedProcessCount() == 0,
        "Expected zero active slots and zero live managed OS processes after completion"
    );
  }

  @Test
  void invokesMember2NetworkValidatorHookBeforeAllocatingSlotOrLaunchingProcess() throws Exception {
    REJECT_NETWORK.set(true);
    StartTrainingRequest invalidReq = TrainingTestFixtures.sampleStartRequest("points-2d", 16, 2);

    mockMvc.perform(post("/api/training/start")
            .header(HttpHeaders.AUTHORIZATION, bearerToken)
            .header("X-Request-Id", "req-net-invalid")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(invalidReq)))
        .andExpect(status().isUnprocessableEntity())
        .andExpect(header().string("X-Request-Id", "req-net-invalid"))
        .andExpect(jsonPath("$.code").value("INVALID_NETWORK_GRAPH"))
        .andExpect(jsonPath("$.details.datasetId").value("points-2d"));

    assertThat(jobService.activeSlotCount()).isEqualTo(0);
    assertThat(jobService.activeManagedProcessCount()).isEqualTo(0);
  }
}
