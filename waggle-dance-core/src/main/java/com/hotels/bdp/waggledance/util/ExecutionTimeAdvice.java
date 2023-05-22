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
package com.hotels.bdp.waggledance.util;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.stream.Collectors;


@Aspect
@Component
public class ExecutionTimeAdvice {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionTimeAdvice.class);

    @Around("@annotation(com.hotels.bdp.waggledance.util.TrackExecutionTime)")
    public Object executionTime(ProceedingJoinPoint point) throws Throwable {
        LOG.info("Entering Class name: " + point.getSignature().getDeclaringTypeName() + ", Method Name: " + point.getSignature().getName() + ", Parameters: " + Arrays.stream(point.getArgs()).map(Object::toString).collect(Collectors.joining()));
        long startTime = System.currentTimeMillis();
        Object object = point.proceed();
        long endTime = System.currentTimeMillis();
        LOG.info("Exiting Class Name: "+ point.getSignature().getDeclaringTypeName() +", Method Name: "+ point.getSignature().getName() + ", Time taken for Execution is : " + (endTime-startTime) +"ms");
        return object;
    }
}
