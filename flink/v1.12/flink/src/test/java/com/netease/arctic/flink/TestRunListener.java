package com.netease.arctic.flink;

import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.RunListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRunListener extends RunListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestRunListener.class);

  @Override
  public void testRunStarted(Description description) throws Exception {
    LOGGER.info("------------------>>>>testRunStarted {} {} {}------------------------------", description.getClassName(),
        description.getDisplayName(), description);
  }

  @Override
  public void testStarted(Description description) throws Exception {
    LOGGER.info("-------------->>>>testStarted {}", description);
  }

  @Override
  public void testFinished(Description description) throws Exception {
    LOGGER.info("-------------<<<<<testFinished {}", description);
  }

  @Override
  public void testRunFinished(Result result) throws Exception {
    LOGGER.info("-----------------<<<<<testRunFinished {} time:{} R{} F{} I{}", result, result.getRunTime(),
        result.getRunCount(), result.getFailureCount(), result.getIgnoreCount());
  }
}
