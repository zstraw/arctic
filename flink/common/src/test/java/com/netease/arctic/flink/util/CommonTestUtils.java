/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.flink.util;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Copy some useful code from flink to avoid flink dependency.
 */
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
