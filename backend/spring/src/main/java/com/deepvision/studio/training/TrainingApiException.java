package com.deepvision.studio.training;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;

public class TrainingApiException extends RuntimeException {
  private final HttpStatus httpStatus;
  private final String code;
  private final Map<String, Object> details;

  public TrainingApiException(HttpStatus httpStatus, String code, String message) {
    this(httpStatus, code, message, Map.of());
  }

  public TrainingApiException(HttpStatus httpStatus, String code, String message, Map<String, Object> details) {
    super(message);
    this.httpStatus = httpStatus;
    this.code = code;
    this.details = details == null || details.isEmpty()
        ? Map.of()
        : Collections.unmodifiableMap(new LinkedHashMap<>(details));
  }

  public HttpStatus httpStatus() {
    return httpStatus;
  }

  public String code() {
    return code;
  }

  public Map<String, Object> details() {
    return details;
  }
}
