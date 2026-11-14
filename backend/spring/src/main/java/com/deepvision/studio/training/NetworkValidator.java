package com.deepvision.studio.training;

import com.deepvision.studio.training.TrainingDtos.StartTrainingRequest;
import com.deepvision.studio.training.TrainingDtos.TrainingDatasetDetail;

/**
 * Extension hook reserved for Member 2 (network structure & shape validation).
 * Invoked before reserving capacity or spawning a training worker subprocess.
 */
public interface NetworkValidator {
  void validate(StartTrainingRequest request, TrainingDatasetDetail dataset);
}
