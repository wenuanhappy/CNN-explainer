package com.deepvision.studio.training;

import java.io.IOException;
import java.nio.file.Path;

public interface TrainingProcessLauncher {
  record WorkerLaunchRequest(
      String jobId,
      Path jobDirectory,
      Path requestFile,
      Path controlFile,
      Path checkpointFile
  ) {}

  ManagedWorkerProcess launch(WorkerLaunchRequest request) throws IOException;
}
