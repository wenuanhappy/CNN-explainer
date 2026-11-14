package com.deepvision.studio.training;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TrainingValidationConfig {
  @Bean
  @ConditionalOnMissingBean(NetworkValidator.class)
  public NetworkValidator networkValidator() {
    return (request, dataset) -> {
      // Default pass-through validator until Member 2 registers a concrete NetworkValidator bean.
    };
  }
}
