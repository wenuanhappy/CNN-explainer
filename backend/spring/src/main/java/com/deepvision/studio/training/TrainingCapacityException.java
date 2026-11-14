package com.deepvision.studio.training;

import java.util.Map;
import org.springframework.http.HttpStatus;

public class TrainingCapacityException extends TrainingApiException {
  public TrainingCapacityException(String code, String message, Map<String, Object> details) {
    super(HttpStatus.TOO_MANY_REQUESTS, code, message, details);
  }
}
