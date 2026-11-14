package com.deepvision.studio.training;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Zero-dependency standalone OS subprocess worker used by integration tests to exercise
 * {@link DefaultTrainingProcessLauncher}, real OS process lifecycle, file-based control protocol,
 * stdout JSON event streaming, and checkpoint file creation without requiring a GPU.
 */
public final class FakeWorkerMain {
  private static final Pattern JOB_ID_PATTERN = Pattern.compile("\"jobId\"\\s*:\\s*\"([^\"]+)\"");
  private static final Pattern CONTROL_FILE_PATTERN =
      Pattern.compile("\"controlFile\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"");
  private static final Pattern CHECKPOINT_FILE_PATTERN =
      Pattern.compile("\"checkpointFile\"\\s*:\\s*\"((?:\\\\.|[^\"\\\\])*)\"");
  private static final Pattern TOTAL_EPOCHS_PATTERN = Pattern.compile("\"totalEpochs\"\\s*:\\s*(\\d+)");
  private static final Pattern COMMAND_PATTERN = Pattern.compile("\"command\"\\s*:\\s*\"([^\"]+)\"");

  private FakeWorkerMain() {}

  public static void main(String[] args) throws Exception {
    Path requestPath = parseRequestArg(args);
    String requestJson = Files.readString(requestPath, StandardCharsets.UTF_8);

    String jobId = extractRequired(JOB_ID_PATTERN, requestJson, "jobId");
    Path controlFile = Path.of(unescapeJsonString(extractRequired(CONTROL_FILE_PATTERN, requestJson, "controlFile")));
    Path checkpointFile =
        Path.of(unescapeJsonString(extractRequired(CHECKPOINT_FILE_PATTERN, requestJson, "checkpointFile")));
    int totalEpochs = Integer.parseInt(extractOptional(TOTAL_EPOCHS_PATTERN, requestJson, "2"));

    emitControl(jobId, "running", "Fake OS subprocess worker started on cpu.");

    long stepSleepMs = totalEpochs >= 30 ? 40L : 15L;
    for (int epoch = 1; epoch <= totalEpochs; epoch++) {
      if (!checkControlOrWaitIfPaused(jobId, controlFile)) {
        emitControl(jobId, "stopped", "Training stopped by control file.");
        return;
      }
      Thread.sleep(stepSleepMs);
      if (!checkControlOrWaitIfPaused(jobId, controlFile)) {
        emitControl(jobId, "stopped", "Training stopped by control file.");
        return;
      }
      double loss = Math.max(0.05, 0.9 - epoch * 0.15);
      double acc = Math.min(0.99, 0.55 + epoch * 0.12);
      emitLine(String.format(
          Locale.US,
          "{\"type\":\"metric\",\"jobId\":\"%s\",\"device\":\"cpu\",\"step\":%d,\"epoch\":%d,\"batch\":1,"
              + "\"totalEpochs\":%d,\"totalBatches\":1,\"loss\":%.4f,\"valLoss\":%.4f,"
              + "\"accuracy\":%.4f,\"valAccuracy\":%.4f,\"lr\":0.001,\"elapsedSeconds\":%d,"
              + "\"etaSeconds\":%d,\"gradientNorm\":0.11,\"weightMean\":0.02,\"weightStd\":0.14,"
              + "\"gradientStatus\":\"healthy\"}",
          escapeJson(jobId),
          epoch,
          epoch,
          totalEpochs,
          loss,
          loss + 0.03,
          acc,
          acc - 0.02,
          epoch,
          Math.max(0, totalEpochs - epoch)
      ));
    }

    if (checkpointFile.getParent() != null) {
      Files.createDirectories(checkpointFile.getParent());
    }
    Files.writeString(
        checkpointFile,
        "{\"fakeCheckpoint\":true,\"jobId\":\"" + escapeJson(jobId) + "\"}",
        StandardCharsets.UTF_8
    );

    emitLine(String.format(
        Locale.US,
        "{\"type\":\"test_result\",\"jobId\":\"%s\",\"device\":\"cpu\",\"testLoss\":0.1625,"
            + "\"testAccuracy\":0.9375,\"sampleCount\":16,\"samples\":[]}",
        escapeJson(jobId)
    ));
    emitControl(jobId, "completed", "Training completed cleanly.");
  }

  private static boolean checkControlOrWaitIfPaused(String jobId, Path controlFile)
      throws InterruptedException {
    String cmd = readCommand(controlFile);
    if ("stopped".equals(cmd)) {
      return false;
    }
    if ("paused".equals(cmd)) {
      emitControl(jobId, "paused", "Training paused.");
      while (true) {
        Thread.sleep(20L);
        String next = readCommand(controlFile);
        if ("stopped".equals(next)) {
          return false;
        }
        if ("running".equals(next)) {
          emitControl(jobId, "running", "Training resumed.");
          return true;
        }
      }
    }
    return true;
  }

  private static String readCommand(Path controlFile) {
    try {
      if (!Files.isRegularFile(controlFile)) {
        return "running";
      }
      String text = Files.readString(controlFile, StandardCharsets.UTF_8);
      Matcher m = COMMAND_PATTERN.matcher(text);
      return m.find() ? m.group(1) : "running";
    } catch (IOException ignored) {
      return "running";
    }
  }

  private static void emitControl(String jobId, String status, String message) {
    emitLine(String.format(
        Locale.US,
        "{\"type\":\"control\",\"jobId\":\"%s\",\"status\":\"%s\",\"device\":\"cpu\",\"message\":\"%s\"}",
        escapeJson(jobId),
        escapeJson(status),
        escapeJson(message)
    ));
  }

  private static void emitLine(String json) {
    System.out.println(json);
    System.out.flush();
  }

  private static Path parseRequestArg(String[] args) {
    for (int i = 0; i < args.length - 1; i++) {
      if ("--request".equals(args[i])) {
        return Path.of(args[i + 1]);
      }
    }
    throw new IllegalArgumentException("Missing --request <path> argument.");
  }

  private static String extractRequired(Pattern pattern, String json, String fieldName) {
    Matcher m = pattern.matcher(json);
    if (!m.find()) {
      throw new IllegalArgumentException("Missing required field in request.json: " + fieldName);
    }
    return m.group(1);
  }

  private static String extractOptional(Pattern pattern, String json, String fallback) {
    Matcher m = pattern.matcher(json);
    return m.find() ? m.group(1) : fallback;
  }

  private static String unescapeJsonString(String raw) {
    return raw.replace("\\\\", "\\").replace("\\\"", "\"").replace("\\/", "/");
  }

  private static String escapeJson(String raw) {
    return raw.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
