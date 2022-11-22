package com.netease.arctic.utils.junit;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogExtension implements BeforeAllCallback, BeforeEachCallback, AfterAllCallback, AfterEachCallback  {
  
  public static final Logger LOG = LoggerFactory.getLogger(LogExtension.class);
  
  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    LOG.info("================={} Tests finished!===================", context.getRequiredTestClass());
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    LOG.info("=================={} test is finished.============", context.getRequiredTestMethod());
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    LOG.info("================={} Tests started!===================", context.getRequiredTestClass());
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    LOG.info("=================={} test is starting...============", context.getRequiredTestMethod());
  }
}
