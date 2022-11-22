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

import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.platform.commons.util.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.List;
import java.util.Optional;

/**
 * wrap parameter in {@link org.junit.jupiter.params.ParameterizedTest}
 */
public class MappedParameterContext implements ParameterContext {

  private final int index;
  private final Parameter parameter;
  private final Optional<Object> target;

  public MappedParameterContext(int index, Parameter parameter,
                                Optional<Object> target) {
    this.index = index;
    this.parameter = parameter;
    this.target = target;
  }

  @Override
  public boolean isAnnotated(Class<? extends Annotation> annotationType) {
    return AnnotationUtils.isAnnotated(parameter, annotationType);
  }

  @Override
  public <A extends Annotation> Optional<A> findAnnotation(Class<A> annotationType) {
    return Optional.empty();
  }

  @Override
  public <A extends Annotation> List<A> findRepeatableAnnotations(Class<A> annotationType) {
    return null;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public Parameter getParameter() {
    return parameter;
  }

  @Override
  public Optional<Object> getTarget() {
    return target;
  }
}