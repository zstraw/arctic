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

import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class MiniClusterResource {
  
  private static final Logger LOG = LoggerFactory.getLogger(MiniClusterResource.class);
  
  public static MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      org.apache.iceberg.flink.MiniClusterResource.createWithClassloaderCheckDisabled();
  public static AtomicBoolean start = new AtomicBoolean(false);

  public void before() throws Exception {
    LOG.info("minicluster start :{}", start);
    if (!start.getAndSet(true)) {
      MINI_CLUSTER_RESOURCE.before();
    }
  }

  public void after() {
    MINI_CLUSTER_RESOURCE.after();
  }
  
}
