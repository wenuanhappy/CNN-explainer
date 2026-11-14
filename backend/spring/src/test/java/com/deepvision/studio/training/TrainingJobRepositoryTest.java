package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.ActiveProfiles;

@DataJpaTest
@ActiveProfiles("test")
class TrainingJobRepositoryTest {

  @Autowired
  private TrainingJobRepository repository;

  @Test
  void persistsAndLoadsAllJobMetadataFields() {
    Instant created = Instant.parse("2026-11-14T05:00:00Z");
    Instant started = Instant.parse("2026-11-14T05:00:02Z");
    Instant finished = Instant.parse("2026-11-14T05:01:00Z");

    TrainingJob job = new TrainingJob(
        "train-20261114-0001",
        "alice",
        "iris",
        "a".repeat(64),
        "b".repeat(64),
        "running",
        "idem-001",
        10,
        4,
        "{\"datasetId\":\"iris\"}",
        created
    );
    job.setStartedAt(started);
    job.setDevice("cpu");
    job.setLastSequence(12L);
    job.setExitCode(0);
    job.setStatus("completed");
    job.setFinishedAt(finished);
    job.setUpdatedAt(finished);
    repository.saveAndFlush(job);

    TrainingJob loaded = repository.findByJobIdAndOwner("train-20261114-0001", "alice").orElseThrow();
    assertThat(loaded.getOwner()).isEqualTo("alice");
    assertThat(loaded.getDatasetId()).isEqualTo("iris");
    assertThat(loaded.getConfigDigest()).isEqualTo("a".repeat(64));
    assertThat(loaded.getStatus()).isEqualTo("completed");
    assertThat(loaded.getCreatedAt()).isEqualTo(created);
    assertThat(loaded.getStartedAt()).isEqualTo(started);
    assertThat(loaded.getFinishedAt()).isEqualTo(finished);
    assertThat(loaded.getUpdatedAt()).isEqualTo(finished);
    assertThat(loaded.getLastSequence()).isEqualTo(12L);
    assertThat(loaded.getExitCode()).isZero();
    assertThat(loaded.getDevice()).isEqualTo("cpu");
    assertThat(loaded.getIdempotencyKey()).isEqualTo("idem-001");
  }

  @Test
  void allowsMultipleNullIdempotencyKeysForSameOwnerAndEnforcesUniquePerOwnerKey() {
    Instant now = Instant.now();
    TrainingJob firstNoKey = new TrainingJob(
        "train-no-key-1", "alice", "iris", "1".repeat(64), "sig1", "queued", null, 5, 2, "{}", now
    );
    TrainingJob secondNoKey = new TrainingJob(
        "train-no-key-2", "alice", "iris", "1".repeat(64), "sig1", "running", null, 5, 2, "{}", now.plusSeconds(1)
    );
    repository.saveAndFlush(firstNoKey);
    repository.saveAndFlush(secondNoKey);

    assertThat(repository.countByOwnerAndStatusIn("alice", List.of("queued", "running", "paused"))).isEqualTo(2L);

    TrainingJob aliceKeyed = new TrainingJob(
        "train-key-alice-1", "alice", "iris", "2".repeat(64), "sig2", "running", "shared-key", 5, 2, "{}", now.plusSeconds(2)
    );
    TrainingJob bobKeyed = new TrainingJob(
        "train-key-bob-1", "bob", "iris", "2".repeat(64), "sig2", "running", "shared-key", 5, 2, "{}", now.plusSeconds(3)
    );
    repository.saveAndFlush(aliceKeyed);
    repository.saveAndFlush(bobKeyed);

    assertThat(repository.findByOwnerAndIdempotencyKey("alice", "shared-key"))
        .map(TrainingJob::getJobId)
        .contains("train-key-alice-1");
    assertThat(repository.findByOwnerAndIdempotencyKey("bob", "shared-key"))
        .map(TrainingJob::getJobId)
        .contains("train-key-bob-1");

    TrainingJob duplicateAliceKey = new TrainingJob(
        "train-key-alice-dup", "alice", "iris", "2".repeat(64), "sig2", "queued", "shared-key", 5, 2, "{}", now.plusSeconds(4)
    );
    assertThatThrownBy(() -> repository.saveAndFlush(duplicateAliceKey))
        .isInstanceOf(DataIntegrityViolationException.class);
  }
}
