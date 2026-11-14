package com.deepvision.studio.training;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "deepvision.training")
public class TrainingProperties {
  private String streamBaseUrl = "";
  private String jobsRoot = "./training-jobs";
  private String pythonExecutable = "python";
  private String workerScript = "../python-training/training_worker.py";
  private List<String> workerCommandPrefix = new ArrayList<>();
  private int maxConcurrentJobs = 1;
  private int maxActiveJobsPerUser = 2;
  private int maxQueueSize = 4;
  private Duration jobTimeout = Duration.ofMinutes(30);
  private Duration stopTimeout = Duration.ofSeconds(10);
  private Duration terminalRuntimeRetention = Duration.ofMinutes(10);
  private int maxHistoryEvents = 240;
  private boolean releaseSlotOnPause = false;

  public String getStreamBaseUrl() { return streamBaseUrl; }
  public void setStreamBaseUrl(String streamBaseUrl) { this.streamBaseUrl = streamBaseUrl; }

  public String getJobsRoot() { return jobsRoot; }
  public void setJobsRoot(String jobsRoot) { this.jobsRoot = jobsRoot; }

  public String getPythonExecutable() { return pythonExecutable; }
  public void setPythonExecutable(String pythonExecutable) { this.pythonExecutable = pythonExecutable; }

  public String getWorkerScript() { return workerScript; }
  public void setWorkerScript(String workerScript) { this.workerScript = workerScript; }

  public List<String> getWorkerCommandPrefix() { return workerCommandPrefix; }
  public void setWorkerCommandPrefix(List<String> workerCommandPrefix) {
    this.workerCommandPrefix = workerCommandPrefix == null ? new ArrayList<>() : new ArrayList<>(workerCommandPrefix);
  }

  public int getMaxConcurrentJobs() { return Math.max(1, maxConcurrentJobs); }
  public void setMaxConcurrentJobs(int maxConcurrentJobs) { this.maxConcurrentJobs = maxConcurrentJobs; }

  public int getMaxActiveJobsPerUser() { return Math.max(1, maxActiveJobsPerUser); }
  public void setMaxActiveJobsPerUser(int maxActiveJobsPerUser) { this.maxActiveJobsPerUser = maxActiveJobsPerUser; }

  public int getMaxQueueSize() { return Math.max(0, maxQueueSize); }
  public void setMaxQueueSize(int maxQueueSize) { this.maxQueueSize = maxQueueSize; }

  public Duration getJobTimeout() { return jobTimeout == null ? Duration.ofMinutes(30) : jobTimeout; }
  public void setJobTimeout(Duration jobTimeout) { this.jobTimeout = jobTimeout; }

  public Duration getStopTimeout() { return stopTimeout == null ? Duration.ofSeconds(10) : stopTimeout; }
  public void setStopTimeout(Duration stopTimeout) { this.stopTimeout = stopTimeout; }

  public Duration getTerminalRuntimeRetention() {
    return terminalRuntimeRetention == null ? Duration.ofMinutes(10) : terminalRuntimeRetention;
  }
  public void setTerminalRuntimeRetention(Duration terminalRuntimeRetention) {
    this.terminalRuntimeRetention = terminalRuntimeRetention;
  }

  public int getMaxHistoryEvents() { return Math.max(1, maxHistoryEvents); }
  public void setMaxHistoryEvents(int maxHistoryEvents) { this.maxHistoryEvents = maxHistoryEvents; }

  public boolean isReleaseSlotOnPause() { return releaseSlotOnPause; }
  public void setReleaseSlotOnPause(boolean releaseSlotOnPause) { this.releaseSlotOnPause = releaseSlotOnPause; }
}
