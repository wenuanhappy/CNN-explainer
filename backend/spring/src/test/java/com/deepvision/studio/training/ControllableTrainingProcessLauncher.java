package com.deepvision.studio.training;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ControllableTrainingProcessLauncher implements TrainingProcessLauncher {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final AtomicInteger launchCount = new AtomicInteger();
  private final Map<String, FakeWorkerProcess> processesByJobId = new ConcurrentHashMap<>();
  private final List<FakeWorkerProcess> allProcesses = new CopyOnWriteArrayList<>();
  private volatile Function<WorkerLaunchRequest, IOException> failureInjector;
  private volatile Function<WorkerLaunchRequest, RuntimeException> runtimeFailureInjector;

  @Override
  public ManagedWorkerProcess launch(WorkerLaunchRequest request) throws IOException {
    launchCount.incrementAndGet();
    Function<WorkerLaunchRequest, RuntimeException> rtInjector = runtimeFailureInjector;
    if (rtInjector != null) {
      RuntimeException rtFailure = rtInjector.apply(request);
      if (rtFailure != null) {
        throw rtFailure;
      }
    }
    Function<WorkerLaunchRequest, IOException> injector = failureInjector;
    if (injector != null) {
      IOException failure = injector.apply(request);
      if (failure != null) {
        throw failure;
      }
    }
    FakeWorkerProcess process = new FakeWorkerProcess(request);
    processesByJobId.put(request.jobId(), process);
    allProcesses.add(process);
    return process;
  }

  public void setFailureInjector(Function<WorkerLaunchRequest, IOException> failureInjector) {
    this.failureInjector = failureInjector;
  }

  public void setRuntimeFailureInjector(Function<WorkerLaunchRequest, RuntimeException> runtimeFailureInjector) {
    this.runtimeFailureInjector = runtimeFailureInjector;
  }

  public int totalLaunches() {
    return launchCount.get();
  }

  public int liveProcessCount() {
    int count = 0;
    for (FakeWorkerProcess process : allProcesses) {
      if (process.isAlive()) {
        count++;
      }
    }
    return count;
  }

  public List<FakeWorkerProcess> allProcesses() {
    return List.copyOf(allProcesses);
  }

  public FakeWorkerProcess getProcess(String jobId) {
    return processesByJobId.get(jobId);
  }

  public FakeWorkerProcess awaitProcess(String jobId, Duration timeout) throws InterruptedException {
    Instant deadline = Instant.now().plus(timeout);
    while (Instant.now().isBefore(deadline)) {
      FakeWorkerProcess found = processesByJobId.get(jobId);
      if (found != null) {
        return found;
      }
      Thread.sleep(15);
    }
    throw new AssertionError("Timed out waiting for worker process launch of job " + jobId);
  }

  public static final class FakeWorkerProcess implements ManagedWorkerProcess {
    private static final byte[] EOF = new byte[0];

    private final WorkerLaunchRequest request;
    private final BlockingQueue<byte[]> outputChunks = new LinkedBlockingQueue<>();
    private final InputStream stdout = new QueueInputStream(outputChunks, EOF);
    private final CountDownLatch exitLatch = new CountDownLatch(1);
    private final AtomicBoolean alive = new AtomicBoolean(true);
    private final AtomicBoolean forciblyDestroyed = new AtomicBoolean(false);
    private volatile boolean delayExitOnDestroy = false;
    private volatile int exitCode = 0;

    public FakeWorkerProcess(WorkerLaunchRequest request) {
      this.request = request;
    }

    public WorkerLaunchRequest request() {
      return request;
    }

    public void setDelayExitOnDestroy(boolean delayExitOnDestroy) {
      this.delayExitOnDestroy = delayExitOnDestroy;
    }

    public void completeDelayedExit(int code) {
      this.exitCode = code;
      alive.set(false);
      outputChunks.offer(EOF);
      exitLatch.countDown();
    }

    public String readControlCommand() {
      try {
        if (!Files.exists(request.controlFile())) {
          return null;
        }
        JsonNode node = MAPPER.readTree(Files.readString(request.controlFile(), StandardCharsets.UTF_8));
        return node.path("command").asText(null);
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public void emitTrailingErrorBeforeDelayedExit(String errorCode, String message) {
      try {
        String raw = MAPPER.writeValueAsString(Map.of(
            "type", "error",
            "jobId", request.jobId(),
            "errorCode", errorCode,
            "message", message
        ));
        outputChunks.offer((raw + "\n").getBytes(StandardCharsets.UTF_8));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public void emitLine(String jsonLine) {
      if (alive.get()) {
        outputChunks.offer((jsonLine + "\n").getBytes(StandardCharsets.UTF_8));
      }
    }

    public void emitJson(Map<String, Object> payload) {
      try {
        emitLine(MAPPER.writeValueAsString(payload));
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    public void emitControl(String status, String message) {
      emitJson(Map.of(
          "type", "control",
          "jobId", request.jobId(),
          "status", status,
          "message", message
      ));
    }

    public void emitControlWithDevice(String status, String device, String message) {
      emitJson(Map.of(
          "type", "control",
          "jobId", request.jobId(),
          "status", status,
          "device", device,
          "message", message
      ));
    }

    public void emitError(String errorCode, String message) {
      emitJson(Map.of(
          "type", "error",
          "jobId", request.jobId(),
          "errorCode", errorCode,
          "message", message
      ));
    }

    public void completeCleanly() {
      emitControl("completed", "Training finished.");
      finish(0);
    }

    public void confirmStopAndExit() {
      emitControl("stopped", "Training stopped by user.");
      finish(0);
    }

    public void finish(int code) {
      if (alive.compareAndSet(true, false)) {
        this.exitCode = code;
        outputChunks.offer(EOF);
        exitLatch.countDown();
      }
    }

    public boolean wasForciblyDestroyed() {
      return forciblyDestroyed.get();
    }

    @Override
    public InputStream outputStream() {
      return stdout;
    }

    @Override
    public int waitFor() throws InterruptedException {
      exitLatch.await();
      return exitCode;
    }

    @Override
    public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
      return exitLatch.await(timeout, unit);
    }

    @Override
    public int exitValue() {
      if (alive.get()) {
        throw new IllegalThreadStateException("Fake worker process has not exited.");
      }
      return exitCode;
    }

    @Override
    public boolean isAlive() {
      return alive.get();
    }

    @Override
    public void destroyGracefully(Duration gracePeriod) {
      destroyForcibly();
    }

    @Override
    public void destroyForcibly() {
      forciblyDestroyed.set(true);
      if (delayExitOnDestroy) {
        alive.set(false);
        return;
      }
      finish(137);
    }
  }

  private static final class QueueInputStream extends InputStream {
    private final BlockingQueue<byte[]> queue;
    private final byte[] eofMarker;
    private byte[] currentChunk;
    private int offset;
    private boolean eofReached;

    private QueueInputStream(BlockingQueue<byte[]> queue, byte[] eofMarker) {
      this.queue = queue;
      this.eofMarker = eofMarker;
    }

    @Override
    public int read() throws IOException {
      if (!ensureChunk()) {
        return -1;
      }
      return currentChunk[offset++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (!ensureChunk()) {
        return -1;
      }
      int available = Math.min(len, currentChunk.length - offset);
      System.arraycopy(currentChunk, offset, b, off, available);
      offset += available;
      return available;
    }

    private boolean ensureChunk() throws IOException {
      if (eofReached) {
        return false;
      }
      while (currentChunk == null || offset >= currentChunk.length) {
        try {
          byte[] next = queue.take();
          if (next == eofMarker) {
            eofReached = true;
            currentChunk = null;
            return false;
          }
          currentChunk = next;
          offset = 0;
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while reading worker stdout.", ex);
        }
      }
      return true;
    }
  }
}
