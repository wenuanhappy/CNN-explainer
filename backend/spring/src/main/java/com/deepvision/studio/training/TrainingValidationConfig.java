package com.deepvision.studio.training;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(TrainingProperties.class)
public class TrainingValidationConfig {
  @Bean
  @ConditionalOnMissingBean(NetworkValidator.class)
  public NetworkValidator networkValidator() {
    return (request, dataset) -> {
      // Default pass-through validator until Member 2 registers a concrete NetworkValidator bean.
    };
  }

  @Bean
  @ConditionalOnMissingBean(TrainingProcessLauncher.class)
  public TrainingProcessLauncher trainingProcessLauncher(TrainingProperties properties) {
    return new DefaultTrainingProcessLauncher(properties);
  }
}
