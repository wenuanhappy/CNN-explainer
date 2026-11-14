package com.deepvision.studio.training;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class TrainingJobStateMachineTest {

  @Test
  void enforcesCentralizedStateTransitionTable() {
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.QUEUED, TrainingJobStatus.RUNNING)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.QUEUED, TrainingJobStatus.STOPPED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.QUEUED, TrainingJobStatus.FAILED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.QUEUED, TrainingJobStatus.INTERRUPTED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.QUEUED, TrainingJobStatus.PAUSED)).isFalse();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.QUEUED, TrainingJobStatus.COMPLETED)).isFalse();

    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.RUNNING, TrainingJobStatus.PAUSED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.RUNNING, TrainingJobStatus.COMPLETED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.RUNNING, TrainingJobStatus.STOPPED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.RUNNING, TrainingJobStatus.FAILED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.RUNNING, TrainingJobStatus.INTERRUPTED)).isTrue();

    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.PAUSED, TrainingJobStatus.RUNNING)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.PAUSED, TrainingJobStatus.STOPPED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.PAUSED, TrainingJobStatus.FAILED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.PAUSED, TrainingJobStatus.INTERRUPTED)).isTrue();
    assertThat(TrainingJobStateMachine.canTransition(TrainingJobStatus.PAUSED, TrainingJobStatus.COMPLETED)).isFalse();

    for (TrainingJobStatus terminal : new TrainingJobStatus[]{
        TrainingJobStatus.COMPLETED,
        TrainingJobStatus.STOPPED,
        TrainingJobStatus.FAILED,
        TrainingJobStatus.INTERRUPTED
    }) {
      for (TrainingJobStatus target : TrainingJobStatus.values()) {
        assertThat(TrainingJobStateMachine.canTransition(terminal, target)).isFalse();
        assertThatThrownBy(() -> TrainingJobStateMachine.requireTransition("job-1", terminal, target))
            .isInstanceOf(TrainingStateConflictException.class)
            .satisfies(ex -> {
              TrainingStateConflictException conflict = (TrainingStateConflictException) ex;
              assertThat(conflict.httpStatus()).isEqualTo(HttpStatus.CONFLICT);
              assertThat(conflict.code()).isEqualTo("INVALID_STATE_TRANSITION");
            });
      }
    }
  }

  @Test
  void rejectsIllegalControlRequestsWith409IncludingTerminalResume() {
    assertThatCode(() -> TrainingJobStateMachine.validateControlRequest(
        "job-1", TrainingJobStatus.RUNNING, null, TrainingControlAction.PAUSE
    )).doesNotThrowAnyException();

    assertThatCode(() -> TrainingJobStateMachine.validateControlRequest(
        "job-1", TrainingJobStatus.PAUSED, null, TrainingControlAction.RESUME
    )).doesNotThrowAnyException();

    assertThatCode(() -> TrainingJobStateMachine.validateControlRequest(
        "job-1", TrainingJobStatus.RUNNING, TrainingControlAction.PAUSE, TrainingControlAction.STOP
    )).doesNotThrowAnyException();

    // Terminal resume MUST return 409
    assertThatThrownBy(() -> TrainingJobStateMachine.validateControlRequest(
        "job-1", TrainingJobStatus.COMPLETED, null, TrainingControlAction.RESUME
    ))
        .isInstanceOf(TrainingStateConflictException.class)
        .satisfies(ex -> assertThat(((TrainingStateConflictException) ex).httpStatus()).isEqualTo(HttpStatus.CONFLICT));

    // Pause while stopping -> 409
    assertThatThrownBy(() -> TrainingJobStateMachine.validateControlRequest(
        "job-1", TrainingJobStatus.RUNNING, TrainingControlAction.STOP, TrainingControlAction.PAUSE
    ))
        .isInstanceOf(TrainingStateConflictException.class)
        .satisfies(ex -> assertThat(((TrainingStateConflictException) ex).httpStatus()).isEqualTo(HttpStatus.CONFLICT));

    // Resume while running -> 409
    assertThatThrownBy(() -> TrainingJobStateMachine.validateControlRequest(
        "job-1", TrainingJobStatus.RUNNING, null, TrainingControlAction.RESUME
    ))
        .isInstanceOf(TrainingStateConflictException.class);
  }

  @Test
  void pausedJobsContinueToOccupyExecutionSlotByDefault() {
    assertThat(TrainingJobStateMachine.occupiesExecutionSlot(TrainingJobStatus.RUNNING, false)).isTrue();
    assertThat(TrainingJobStateMachine.occupiesExecutionSlot(TrainingJobStatus.PAUSED, false)).isTrue();
    assertThat(TrainingJobStateMachine.occupiesExecutionSlot(TrainingJobStatus.PAUSED, true)).isFalse();
    assertThat(TrainingJobStateMachine.occupiesExecutionSlot(TrainingJobStatus.QUEUED, false)).isFalse();
    assertThat(TrainingJobStateMachine.occupiesExecutionSlot(TrainingJobStatus.COMPLETED, false)).isFalse();
    assertThat(TrainingJobStateMachine.occupiesExecutionSlot(TrainingJobStatus.STOPPED, false)).isFalse();
    assertThat(TrainingJobStateMachine.occupiesExecutionSlot(TrainingJobStatus.FAILED, false)).isFalse();
    assertThat(TrainingJobStateMachine.occupiesExecutionSlot(TrainingJobStatus.INTERRUPTED, false)).isFalse();
  }
}
