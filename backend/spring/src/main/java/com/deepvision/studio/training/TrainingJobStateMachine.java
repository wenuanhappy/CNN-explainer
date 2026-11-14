package com.deepvision.studio.training;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class TrainingJobStateMachine {
  private static final Map<TrainingJobStatus, Set<TrainingJobStatus>> ALLOWED_TRANSITIONS;

  static {
    Map<TrainingJobStatus, Set<TrainingJobStatus>> table = new EnumMap<>(TrainingJobStatus.class);
    table.put(TrainingJobStatus.QUEUED, Collections.unmodifiableSet(EnumSet.of(
        TrainingJobStatus.RUNNING,
        TrainingJobStatus.STOPPED,
        TrainingJobStatus.FAILED,
        TrainingJobStatus.INTERRUPTED
    )));
    table.put(TrainingJobStatus.RUNNING, Collections.unmodifiableSet(EnumSet.of(
        TrainingJobStatus.PAUSED,
        TrainingJobStatus.COMPLETED,
        TrainingJobStatus.STOPPED,
        TrainingJobStatus.FAILED,
        TrainingJobStatus.INTERRUPTED
    )));
    table.put(TrainingJobStatus.PAUSED, Collections.unmodifiableSet(EnumSet.of(
        TrainingJobStatus.RUNNING,
        TrainingJobStatus.STOPPED,
        TrainingJobStatus.FAILED,
        TrainingJobStatus.INTERRUPTED
    )));
    table.put(TrainingJobStatus.COMPLETED, Set.of());
    table.put(TrainingJobStatus.STOPPED, Set.of());
    table.put(TrainingJobStatus.FAILED, Set.of());
    table.put(TrainingJobStatus.INTERRUPTED, Set.of());
    ALLOWED_TRANSITIONS = Collections.unmodifiableMap(table);
  }

  private TrainingJobStateMachine() {}

  public static Map<TrainingJobStatus, Set<TrainingJobStatus>> transitionTable() {
    return ALLOWED_TRANSITIONS;
  }

  public static boolean canTransition(TrainingJobStatus from, TrainingJobStatus to) {
    if (from == null || to == null) {
      return false;
    }
    return ALLOWED_TRANSITIONS.getOrDefault(from, Set.of()).contains(to);
  }

  public static void requireTransition(String jobId, TrainingJobStatus from, TrainingJobStatus to) {
    if (!canTransition(from, to)) {
      Map<String, Object> details = new LinkedHashMap<>();
      details.put("jobId", jobId);
      details.put("currentStatus", from == null ? null : from.wireValue());
      details.put("targetStatus", to == null ? null : to.wireValue());
      throw new TrainingStateConflictException(
          "INVALID_STATE_TRANSITION",
          "Cannot transition training job " + jobId + " from " + (from == null ? "null" : from.wireValue())
              + " to " + (to == null ? "null" : to.wireValue()) + ".",
          details
      );
    }
  }

  public static void validateControlRequest(
      String jobId,
      TrainingJobStatus currentStatus,
      TrainingControlAction pendingControl,
      TrainingControlAction requestedAction
  ) {
    if (currentStatus == null || requestedAction == null) {
      throw new IllegalArgumentException("Control status and action are required.");
    }
    if (currentStatus.isTerminal()) {
      throw conflict(
          jobId,
          currentStatus,
          pendingControl,
          requestedAction,
          "Cannot apply '" + requestedAction.wireValue() + "' to terminal training job in status '"
              + currentStatus.wireValue() + "'."
      );
    }

    switch (requestedAction) {
      case PAUSE -> {
        if (currentStatus != TrainingJobStatus.RUNNING
            || (pendingControl != null && pendingControl != TrainingControlAction.PAUSE)) {
          throw conflict(
              jobId,
              currentStatus,
              pendingControl,
              requestedAction,
              "Pause is only allowed when training job is running and not stopping."
          );
        }
      }
      case RESUME -> {
        if (currentStatus != TrainingJobStatus.PAUSED
            || (pendingControl != null && pendingControl != TrainingControlAction.RESUME)) {
          throw conflict(
              jobId,
              currentStatus,
              pendingControl,
              requestedAction,
              "Resume is only allowed when training job is paused and not stopping."
          );
        }
      }
      case STOP -> {
        // Allowed for QUEUED, RUNNING, and PAUSED (even when PAUSE or RESUME is pending).
      }
    }
  }

  /**
   * Resource slot policy:
   * By default (releaseSlotOnPause = false), a PAUSED job continues to hold its execution slot
   * because the Python worker process remains resident holding GPU/CPU memory.
   */
  public static boolean occupiesExecutionSlot(TrainingJobStatus status, boolean releaseSlotOnPause) {
    if (status == TrainingJobStatus.RUNNING) {
      return true;
    }
    if (status == TrainingJobStatus.PAUSED) {
      return !releaseSlotOnPause;
    }
    return false;
  }

  private static TrainingStateConflictException conflict(
      String jobId,
      TrainingJobStatus currentStatus,
      TrainingControlAction pendingControl,
      TrainingControlAction requestedAction,
      String message
  ) {
    Map<String, Object> details = new LinkedHashMap<>();
    details.put("jobId", jobId);
    details.put("currentStatus", currentStatus.wireValue());
    details.put("pendingControl", pendingControl == null ? null : pendingControl.wireValue());
    details.put("requestedAction", requestedAction.wireValue());
    return new TrainingStateConflictException("INVALID_STATE_TRANSITION", message, details);
  }
}
