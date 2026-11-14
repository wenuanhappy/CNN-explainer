package com.deepvision.studio.training;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(
    name = "training_jobs",
    indexes = {
        @Index(name = "idx_training_jobs_owner_created", columnList = "owner_username,created_at"),
        @Index(name = "idx_training_jobs_owner_status", columnList = "owner_username,status"),
        @Index(name = "idx_training_jobs_status_created", columnList = "status,created_at"),
        @Index(name = "idx_training_jobs_owner_idempotency", columnList = "owner_username,idempotency_key")
    }
)
public class TrainingJob {
  @Id
  @Column(name = "job_id", nullable = false, length = 120)
  private String jobId;

  @Column(name = "owner_username", nullable = false, length = 160)
  private String owner;

  @Column(name = "dataset_id", nullable = false, length = 120)
  private String datasetId;

  @Column(name = "config_digest", nullable = false, length = 64)
  private String configDigest;

  @Column(name = "model_signature", length = 80)
  private String modelSignature;

  @Column(name = "status", nullable = false, length = 32)
  private String status;

  @Column(name = "pending_control", length = 32)
  private String pendingControl;

  @Column(name = "created_at", nullable = false)
  private Instant createdAt;

  @Column(name = "started_at")
  private Instant startedAt;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt;

  @Column(name = "finished_at")
  private Instant finishedAt;

  @Column(name = "last_sequence", nullable = false)
  private long lastSequence = 0L;

  @Column(name = "exit_code")
  private Integer exitCode;

  @Column(name = "error_code", length = 64)
  private String errorCode;

  @Column(name = "error_reason", length = 1000)
  private String errorReason;

  @Column(name = "device", length = 64)
  private String device;

  @Column(name = "idempotency_key", length = 128)
  private String idempotencyKey;

  @Column(name = "idempotency_scope_key", unique = true, length = 300)
  private String idempotencyScopeKey;

  @Column(name = "total_epochs", nullable = false)
  private int totalEpochs;

  @Column(name = "total_batches", nullable = false)
  private int totalBatches;

  @Column(name = "current_epoch", nullable = false)
  private int epoch = 0;

  @Column(name = "current_batch", nullable = false)
  private int batch = 0;

  @Column(name = "latest_loss", nullable = false)
  private double latestLoss = 1.7;

  @Column(name = "latest_val_loss")
  private Double latestValLoss = 1.78;

  @Column(name = "latest_accuracy", nullable = false)
  private double latestAccuracy = 0.2;

  @Column(name = "latest_val_accuracy")
  private Double latestValAccuracy = 0.18;

  @Column(name = "elapsed_seconds", nullable = false)
  private long elapsedSeconds = 0L;

  @Column(name = "eta_seconds", nullable = false)
  private long etaSeconds = 0L;

  @Lob
  @Column(name = "request_json")
  private String requestJson;

  protected TrainingJob() {}

  public TrainingJob(
      String jobId,
      String owner,
      String datasetId,
      String configDigest,
      String modelSignature,
      String status,
      String idempotencyKey,
      int totalEpochs,
      int totalBatches,
      String requestJson,
      Instant now
  ) {
    this.jobId = jobId;
    this.owner = owner;
    this.datasetId = datasetId;
    this.configDigest = configDigest;
    this.modelSignature = modelSignature;
    this.status = status;
    setIdempotencyKey(idempotencyKey);
    this.totalEpochs = totalEpochs;
    this.totalBatches = totalBatches;
    this.requestJson = requestJson;
    Instant timestamp = now == null ? Instant.now() : now;
    this.createdAt = timestamp;
    this.updatedAt = timestamp;
  }

  public static String buildIdempotencyScopeKey(String owner, String idempotencyKey) {
    if (owner == null || owner.isBlank() || idempotencyKey == null || idempotencyKey.isBlank()) {
      return null;
    }
    return owner.trim() + ":" + idempotencyKey.trim();
  }

  public String getJobId() { return jobId; }
  public String getOwner() { return owner; }
  public String getDatasetId() { return datasetId; }
  public String getConfigDigest() { return configDigest; }
  public String getModelSignature() { return modelSignature; }
  public String getStatus() { return status; }
  public String getPendingControl() { return pendingControl; }
  public Instant getCreatedAt() { return createdAt; }
  public Instant getStartedAt() { return startedAt; }
  public Instant getUpdatedAt() { return updatedAt; }
  public Instant getFinishedAt() { return finishedAt; }
  public long getLastSequence() { return lastSequence; }
  public Integer getExitCode() { return exitCode; }
  public String getErrorCode() { return errorCode; }
  public String getErrorReason() { return errorReason; }
  public String getDevice() { return device; }
  public String getIdempotencyKey() { return idempotencyKey; }
  public String getIdempotencyScopeKey() { return idempotencyScopeKey; }
  public int getTotalEpochs() { return totalEpochs; }
  public int getTotalBatches() { return totalBatches; }
  public int getEpoch() { return epoch; }
  public int getBatch() { return batch; }
  public double getLatestLoss() { return latestLoss; }
  public Double getLatestValLoss() { return latestValLoss; }
  public double getLatestAccuracy() { return latestAccuracy; }
  public Double getLatestValAccuracy() { return latestValAccuracy; }
  public long getElapsedSeconds() { return elapsedSeconds; }
  public long getEtaSeconds() { return etaSeconds; }
  public String getRequestJson() { return requestJson; }

  public void setStatus(String status) { this.status = status; }
  public void setPendingControl(String pendingControl) { this.pendingControl = pendingControl; }
  public void setStartedAt(Instant startedAt) { this.startedAt = startedAt; }
  public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
  public void setFinishedAt(Instant finishedAt) { this.finishedAt = finishedAt; }
  public void setLastSequence(long lastSequence) { this.lastSequence = lastSequence; }
  public void setExitCode(Integer exitCode) { this.exitCode = exitCode; }
  public void setErrorCode(String errorCode) { this.errorCode = errorCode; }
  public void setErrorReason(String errorReason) {
    this.errorReason = errorReason != null && errorReason.length() > 1000
        ? errorReason.substring(0, 1000)
        : errorReason;
  }
  public void setDevice(String device) { this.device = device; }
  public void setIdempotencyKey(String idempotencyKey) {
    String normalized = (idempotencyKey == null || idempotencyKey.isBlank()) ? null : idempotencyKey.trim();
    this.idempotencyKey = normalized;
    this.idempotencyScopeKey = buildIdempotencyScopeKey(this.owner, normalized);
  }
  public void setTotalBatches(int totalBatches) { this.totalBatches = totalBatches; }
  public void setEpoch(int epoch) { this.epoch = epoch; }
  public void setBatch(int batch) { this.batch = batch; }
  public void setLatestLoss(double latestLoss) { this.latestLoss = latestLoss; }
  public void setLatestValLoss(Double latestValLoss) { this.latestValLoss = latestValLoss; }
  public void setLatestAccuracy(double latestAccuracy) { this.latestAccuracy = latestAccuracy; }
  public void setLatestValAccuracy(Double latestValAccuracy) { this.latestValAccuracy = latestValAccuracy; }
  public void setElapsedSeconds(long elapsedSeconds) { this.elapsedSeconds = elapsedSeconds; }
  public void setEtaSeconds(long etaSeconds) { this.etaSeconds = etaSeconds; }
  public void setRequestJson(String requestJson) { this.requestJson = requestJson; }
}
