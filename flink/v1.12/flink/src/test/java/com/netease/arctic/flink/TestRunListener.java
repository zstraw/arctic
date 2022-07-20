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
    LOGGER.info("testRunStarted " + description.getClassName() + " " + description.getDisplayName() + " "
        + description);
  }

  public void testStarted(Description description) throws Exception {
    LOGGER.info("testStarted " + description.toString());
  }

  public void testFinished(Description description) throws Exception {
    LOGGER.info("testFinished " + description.toString());
  }

  public void testRunFinished(Result result) throws Exception {
    LOGGER.info("testRunFinished " + result.toString()
        + " time:" + result.getRunTime()
        + " R" + result.getRunCount()
        + " F" + result.getFailureCount()
        + " I" + result.getIgnoreCount()
    );
  }
}
