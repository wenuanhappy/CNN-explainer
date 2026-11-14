package com.deepvision.studio.training;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum TrainingControlAction {
  PAUSE("pause", "paused"),
  RESUME("resume", "running"),
  STOP("stop", "stopped");

  private static final Map<String, TrainingControlAction> BY_WIRE = Arrays.stream(values())
      .collect(Collectors.toUnmodifiableMap(TrainingControlAction::wireValue, Function.identity()));

  private final String wireValue;
  private final String workerCommand;

  TrainingControlAction(String wireValue, String workerCommand) {
    this.wireValue = wireValue;
    this.workerCommand = workerCommand;
  }

  @JsonValue
  public String wireValue() {
    return wireValue;
  }

  public String workerCommand() {
    return workerCommand;
  }

  public static TrainingControlAction fromNullableWireValue(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }
    return BY_WIRE.get(raw.trim().toLowerCase());
  }
}
