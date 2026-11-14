package com.deepvision.studio.training;

import java.util.Map;
import org.springframework.http.HttpStatus;

public class TrainingStateConflictException extends TrainingApiException {
  public TrainingStateConflictException(String code, String message, Map<String, Object> details) {
    super(HttpStatus.CONFLICT, code, message, details);
  }
}
