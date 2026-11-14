package com.deepvision.studio.training;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.time.Instant;

@Entity
@Table(
    name = "training_job_events",
    uniqueConstraints = {
        @UniqueConstraint(name = "uk_training_job_events_job_seq", columnNames = {"job_id", "sequence_no"})
    },
    indexes = {
        @Index(name = "idx_training_job_events_job_seq", columnList = "job_id, sequence_no")
    }
)
public class TrainingJobEvent {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @Column(name = "job_id", nullable = false, length = 120)
  private String jobId;

  @Column(name = "sequence_no", nullable = false)
  private long sequence;

  @Column(name = "event_type", nullable = false, length = 48)
  private String eventType;

  @Column(name = "schema_version", nullable = false, length = 16)
  private String schemaVersion;

  @Column(name = "event_timestamp", nullable = false)
  private Instant timestamp;

  @Lob
  @Column(name = "payload_json", nullable = false)
  private String payloadJson;

  protected TrainingJobEvent() {}

  public TrainingJobEvent(
      String jobId,
      long sequence,
      String eventType,
      String schemaVersion,
      Instant timestamp,
      String payloadJson
  ) {
    this.jobId = jobId;
    this.sequence = sequence;
    this.eventType = eventType;
    this.schemaVersion = schemaVersion;
    this.timestamp = timestamp;
    this.payloadJson = payloadJson;
  }

  public Long getId() {
    return id;
  }

  public String getJobId() {
    return jobId;
  }

  public long getSequence() {
    return sequence;
  }

  public String getEventType() {
    return eventType;
  }

  public String getSchemaVersion() {
    return schemaVersion;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getPayloadJson() {
    return payloadJson;
  }
}
