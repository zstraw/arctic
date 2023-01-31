package com.netease.arctic.ams.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ThreadInfo;
import java.util.Map;

public class DeadlockLogHandler implements DeadlockHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DeadlockLogHandler.class);
  
  @Override
  public void handleDeadlock(final ThreadInfo[] deadlockedThreads) {
    if (deadlockedThreads != null) {
      LOG.info("Deadlock detected!");

      Map<Thread, StackTraceElement[]> stackTraceMap = Thread.getAllStackTraces();
      for (ThreadInfo threadInfo : deadlockedThreads) {

        if (threadInfo != null) {

          for (Thread thread : stackTraceMap.keySet()) {

            if (thread.getId() == threadInfo.getThreadId()) {
              LOG.info(threadInfo.toString().trim());

              for (StackTraceElement ste : thread.getStackTrace()) {
                LOG.info("\t" + ste.toString().trim());
              }
            }
          }
        }
      }
    }
  }
}