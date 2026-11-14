package com.deepvision.studio.training;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum TrainingJobStatus {
  QUEUED("queued", false),
  RUNNING("running", false),
  PAUSED("paused", false),
  COMPLETED("completed", true),
  STOPPED("stopped", true),
  FAILED("failed", true),
  INTERRUPTED("interrupted", true);

  private static final Map<String, TrainingJobStatus> BY_WIRE = Arrays.stream(values())
      .collect(Collectors.toUnmodifiableMap(TrainingJobStatus::wireValue, Function.identity()));

  private static final List<String> ACTIVE_WIRE_VALUES = Arrays.stream(values())
      .filter(TrainingJobStatus::isActive)
      .map(TrainingJobStatus::wireValue)
      .toList();

  private final String wireValue;
  private final boolean terminal;

  TrainingJobStatus(String wireValue, boolean terminal) {
    this.wireValue = wireValue;
    this.terminal = terminal;
  }

  @JsonValue
  public String wireValue() {
    return wireValue;
  }

  public boolean isTerminal() {
    return terminal;
  }

  public boolean isActive() {
    return !terminal;
  }

  public static List<String> activeWireValues() {
    return ACTIVE_WIRE_VALUES;
  }

  public static TrainingJobStatus fromWireValue(String raw) {
    if (raw == null || raw.isBlank()) {
      throw new IllegalArgumentException("Training job status cannot be blank.");
    }
    TrainingJobStatus status = BY_WIRE.get(raw.trim().toLowerCase());
    if (status == null) {
      throw new IllegalArgumentException("Unknown training job status: " + raw);
    }
    return status;
  }
}
