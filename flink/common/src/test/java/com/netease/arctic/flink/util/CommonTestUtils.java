package com.netease.arctic.flink.util;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class CommonTestUtils {

  public static void waitUtil(Supplier<Boolean> condition, Duration timeout, String errorMsg) throws TimeoutException, InterruptedException {
    long timeoutMs = timeout.toMillis();
    if (timeoutMs <= 0L) {
      throw new IllegalArgumentException("The timeout must be positive.");
    } else {
      long startingTime = System.currentTimeMillis();

      while (!(Boolean) condition.get() && System.currentTimeMillis() - startingTime < timeoutMs) {
        Thread.sleep(1L);
      }

      if (!(Boolean) condition.get()) {
        throw new TimeoutException(errorMsg);
      }
    }
  }

}
