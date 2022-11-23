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

package com.netease.arctic.utils.junit;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log unit test progress.
 */
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
