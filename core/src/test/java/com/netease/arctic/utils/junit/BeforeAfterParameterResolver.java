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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.engine.execution.BeforeEachMethodAdapter;
import org.junit.jupiter.engine.extension.ExtensionRegistry;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Optional;

/**
 * To support access parameters in {@link BeforeEach} and {@link AfterEach} for junit 5.
 */
public class BeforeAfterParameterResolver implements BeforeEachMethodAdapter, ParameterResolver {

  private ParameterResolver parameterisedTestParameterResolver = null;

  @Override
  public void invokeBeforeEachMethod(ExtensionContext context, ExtensionRegistry registry)
      throws Throwable {
    Optional<ParameterResolver> resolverOptional = registry.getExtensions(ParameterResolver.class)
        .stream()
        .filter(parameterResolver ->
            parameterResolver.getClass().getName()
                .contains("ParameterizedTestParameterResolver")
        )
        .findFirst();
    if (!resolverOptional.isPresent()) {
      throw new IllegalStateException(
          "ParameterizedTestParameterResolver missed in the registry. Probably it's not a Parameterized Test");
    } else {
      parameterisedTestParameterResolver = resolverOptional.get();
    }
  }

  @Override
  public boolean supportsParameter(ParameterContext parameterContext,
                                   ExtensionContext extensionContext) throws ParameterResolutionException {
    if (isExecutedOnAfterOrBeforeMethod(parameterContext)) {
      ParameterContext pContext = getMappedContext(parameterContext, extensionContext);
      return parameterisedTestParameterResolver.supportsParameter(pContext, extensionContext);
    }
    return false;
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext,
                                 ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterisedTestParameterResolver.resolveParameter(
        getMappedContext(parameterContext, extensionContext), extensionContext);
  }

  private MappedParameterContext getMappedContext(ParameterContext parameterContext,
                                                  ExtensionContext extensionContext) {
    return new MappedParameterContext(
        parameterContext.getIndex(),
        extensionContext.getRequiredTestMethod().getParameters()[parameterContext.getIndex()],
        Optional.of(parameterContext.getTarget()));
  }

  private boolean isExecutedOnAfterOrBeforeMethod(ParameterContext parameterContext) {
    return Arrays.stream(parameterContext.getDeclaringExecutable().getDeclaredAnnotations())
        .anyMatch(this::isAfterEachOrBeforeEachAnnotation);
  }

  private boolean isAfterEachOrBeforeEachAnnotation(Annotation annotation) {
    return annotation.annotationType() == BeforeEach.class
        || annotation.annotationType() == AfterEach.class;
  }
}