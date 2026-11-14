package com.deepvision.studio.training;

import java.io.InputStream;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public interface ManagedWorkerProcess {
  InputStream outputStream();

  int waitFor() throws InterruptedException;

  boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException;

  int exitValue();

  boolean isAlive();

  void destroyGracefully(Duration gracePeriod);

  void destroyForcibly();
}
