package com.deepvision.studio.training;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DefaultTrainingProcessLauncher implements TrainingProcessLauncher {
  private final String pythonExecutable;
  private final Path workerScript;
  private final List<String> workerCommandPrefix;

  public DefaultTrainingProcessLauncher(TrainingProperties properties) {
    this.pythonExecutable = properties.getPythonExecutable();
    this.workerScript = Path.of(properties.getWorkerScript()).toAbsolutePath().normalize();
    this.workerCommandPrefix = List.copyOf(properties.getWorkerCommandPrefix());
  }

  @Override
  public ManagedWorkerProcess launch(WorkerLaunchRequest request) throws IOException {
    List<String> command = new ArrayList<>();
    if (!workerCommandPrefix.isEmpty()) {
      command.addAll(workerCommandPrefix);
    } else {
      command.add(pythonExecutable);
      command.add("-B");
      command.add(workerScript.toString());
    }
    command.add("--request");
    command.add(request.requestFile().toString());

    Path workingDirectory = workerScript.getParent();
    if (workingDirectory == null || !Files.isDirectory(workingDirectory)) {
      workingDirectory = request.jobDirectory();
    }

    Process process = new ProcessBuilder(command)
        .directory(workingDirectory.toFile())
        .redirectErrorStream(true)
        .start();
    return new OsManagedWorkerProcess(process);
  }

  private static final class OsManagedWorkerProcess implements ManagedWorkerProcess {
    private final Process process;

    private OsManagedWorkerProcess(Process process) {
      this.process = process;
    }

    @Override
    public InputStream outputStream() {
      return process.getInputStream();
    }

    @Override
    public int waitFor() throws InterruptedException {
      return process.waitFor();
    }

    @Override
    public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
      return process.waitFor(timeout, unit);
    }

    @Override
    public int exitValue() {
      return process.exitValue();
    }

    @Override
    public boolean isAlive() {
      return process.isAlive();
    }

    @Override
    public void destroyGracefully(Duration gracePeriod) {
      if (!process.isAlive()) {
        return;
      }
      process.destroy();
      try {
        long millis = Math.max(50L, gracePeriod == null ? 2000L : gracePeriod.toMillis());
        if (!process.waitFor(millis, TimeUnit.MILLISECONDS)) {
          destroyForcibly();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        destroyForcibly();
      }
    }

    @Override
    public void destroyForcibly() {
      try {
        process.toHandle().descendants().forEach(ProcessHandle::destroyForcibly);
      } catch (RuntimeException ignored) {
        // Best-effort child cleanup across platforms.
      }
      if (process.isAlive()) {
        process.destroyForcibly();
      }
    }
  }
}
