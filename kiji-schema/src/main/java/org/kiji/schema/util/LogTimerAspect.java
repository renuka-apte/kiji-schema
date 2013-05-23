/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.util;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class LogTimerAspect {

  @Pointcut("call(* org.kiji.schema.*.*(..))")
  protected void profile(){
  }

  @Around("profile()")
  public void aroundProfileMethods(final ProceedingJoinPoint thisJoinPoint) throws Throwable {
    Logger LOG = LoggerFactory.getLogger(thisJoinPoint.getTarget().getClass());
    final long start, end;
    start = System.nanoTime();
    thisJoinPoint.proceed();
    end = System.nanoTime();
    System.out.println("calledaspect");
    LOG.info("Time taken by : " +  thisJoinPoint.getSignature().toLongString() + " = ");
    LOG.info(String.format("%l", (end - start) / 1000));
    LOG.info("us");
  }

}