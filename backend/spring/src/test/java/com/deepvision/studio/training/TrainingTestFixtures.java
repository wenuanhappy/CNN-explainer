package com.deepvision.studio.training;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.deepvision.studio.training.TrainingDtos.SplitRequest;
import com.deepvision.studio.training.TrainingDtos.StartTrainingRequest;
import com.deepvision.studio.training.TrainingDtos.TrainingConfigRequest;
import com.deepvision.studio.training.TrainingDtos.TrainingDatasetDetail;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.BooleanSupplier;

final class TrainingTestFixtures {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private TrainingTestFixtures() {}

  static TrainingDatasetService mockDatasetService() {
    TrainingDatasetService service = mock(TrainingDatasetService.class);
    when(service.getDetail(anyString(), anyString())).thenAnswer(invocation -> sampleDataset(invocation.getArgument(0)));
    return service;
  }

  static TrainingDatasetDetail sampleDataset(String datasetId) {
    return new TrainingDatasetDetail(
        datasetId,
        "Synthetic Moons",
        "builtin",
        "points",
        "Synthetic 2D dataset for tests",
        120,
        2,
        "2",
        "0.7/0.15/0.15",
        List.of("0", "1"),
        true,
        0.7,
        0.15,
        0.15,
        List.of(),
        List.of(),
        null,
        List.of(),
        List.of()
    );
  }

  static StartTrainingRequest sampleStartRequest(int batchSize, int totalEpochs) {
    return sampleStartRequest("builtin-moons", batchSize, totalEpochs);
  }

  static StartTrainingRequest sampleStartRequest(String datasetId, int batchSize, int totalEpochs) {
    ObjectNode inputLayer = MAPPER.createObjectNode();
    inputLayer.put("id", 1);
    inputLayer.put("type", "input");
    inputLayer.put("enabled", true);
    inputLayer.putObject("params").put("inputKind", "table").put("featureCount", 2);

    ObjectNode outputLayer = MAPPER.createObjectNode();
    outputLayer.put("id", 2);
    outputLayer.put("type", "output");
    outputLayer.put("enabled", true);
    outputLayer.putObject("params").put("units", 2).put("activation", "softmax");

    List<JsonNode> layers = List.of(inputLayer, outputLayer);
    return new StartTrainingRequest(
        datasetId,
        new SplitRequest(0.7, 0.15, 0.15),
        layers,
        List.of(),
        new TrainingConfigRequest(batchSize, totalEpochs, 0.001, "Adam", "none", 0.9, "cross_entropy")
    );
  }

  static void awaitUntil(Duration timeout, BooleanSupplier condition, String failureMessage)
      throws InterruptedException {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(15);
    }
    if (!condition.getAsBoolean()) {
      throw new AssertionError(failureMessage);
    }
  }
}
