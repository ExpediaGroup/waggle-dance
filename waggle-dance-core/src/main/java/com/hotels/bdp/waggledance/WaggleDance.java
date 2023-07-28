/**
 * Copyright (C) 2016-2023 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.waggledance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.aspectj.EnableSpringConfigured;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.validation.BindException;
import org.springframework.validation.ObjectError;

import lombok.extern.log4j.Log4j2;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.manifest.ManifestAttributes;

@SpringBootApplication
@EnableConfigurationProperties
@EnableSpringConfigured
@EnableAspectJAutoProxy(proxyTargetClass = true)
@Log4j2
public class WaggleDance {

  public interface ContextListener {
    void onStart(ApplicationContext context);

    void onStop(ApplicationContext context);
  }

  private static final List<ContextListener> CONTEXT_LISTENERS = Collections
      .synchronizedList(new ArrayList<>());

  public static void main(String[] args) throws Exception {
    // below is output *before* logging is configured so will appear on console
    logVersionInfo();

    int exitCode = -1;
    try {
      SpringApplication application = new SpringApplicationBuilder(WaggleDance.class)
          .properties("spring.config.location:${server-config:null},${federation-config:null}")
          .properties("server.port:${endpoint.port:18000}")
          .registerShutdownHook(true)
          .build();
      exitCode = SpringApplication.exit(registerListeners(application).run(args));
    } catch (BeanCreationException e) {
      Throwable mostSpecificCause = e.getMostSpecificCause();
      if (mostSpecificCause instanceof BindException) {
        printHelp(((BindException) mostSpecificCause).getAllErrors());
      }
      if (mostSpecificCause instanceof ConstraintViolationException) {
        logConstraintErrors(((ConstraintViolationException) mostSpecificCause));
      }
      throw e;
    }
    if (exitCode != 0) {
      throw new Exception("Waggle Dance didn't exit properly see logs for errors, exitCode=" + exitCode);
    }
  }

  private static SpringApplication registerListeners(SpringApplication application) {
    // Ref:
    // http://docs.spring.io/spring/docs/4.3.x/spring-framework-reference/html/beans.html#context-functionality-events
    application.addListeners((ApplicationListener<ContextRefreshedEvent>) event -> {
      synchronized (CONTEXT_LISTENERS) {
        for (ContextListener contextListener : CONTEXT_LISTENERS) {
          contextListener.onStart(event.getApplicationContext());
        }
      }
    });
    application.addListeners((ApplicationListener<ContextClosedEvent>) event -> {
      synchronized (CONTEXT_LISTENERS) {
        for (ContextListener contextListener : CONTEXT_LISTENERS) {
          contextListener.onStop(event.getApplicationContext());
        }
      }
    });
    return application;
  }

  static @VisibleForTesting void register(ContextListener listener) {
    CONTEXT_LISTENERS.add(listener);
  }

  static @VisibleForTesting void unregister(ContextListener listener) {
    CONTEXT_LISTENERS.remove(listener);
  }

  private static void printHelp(List<ObjectError> allErrors) {
    System.out.println(/* new FetaStoreHelp( */allErrors/* ) */);
  }

  private static void logConstraintErrors(ConstraintViolationException constraintViolationException) {
    log.error("Validation errors:");
    for (ConstraintViolation<?> violation : constraintViolationException.getConstraintViolations()) {
      log.error(violation.toString());
    }
  }

  WaggleDance() {
    // below is output *after* logging is configured so will appear in log file
    logVersionInfo();
  }

  private static void logVersionInfo() {
    ManifestAttributes manifestAttributes = new ManifestAttributes(WaggleDance.class);
    log.info("{}", manifestAttributes);
  }
}
