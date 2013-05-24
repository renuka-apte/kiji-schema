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

import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;

import com.google.common.base.Preconditions;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Aspect
public class LogTimerAspect {
  private HashMap<String, LoggingInfo> mSignatureTimeMap = null;
  private String mPid;

  class LogWriterThread extends Thread {
    @Override
    public void run() {
      try {
        FileWriter fileWriter = new FileWriter("/tmp/logfile", true);
        fileWriter.write(mPid + "\n");
        for (String key: mSignatureTimeMap.keySet()) {
          LoggingInfo loggingInfo = mSignatureTimeMap.get(key);
          fileWriter.write(key + ", " + loggingInfo.toString() + ", "
              + loggingInfo.perCallTime().toString() + "\n");
        }
        fileWriter.close();
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

  class LoggingInfo {
    private Long mAggregateTime;
    private Integer mNumInvoked;

    LoggingInfo() {
      mAggregateTime = 0L;
      mNumInvoked = 0;
    }

    LoggingInfo(long aggregateTime, int timesInvoked) {
      mAggregateTime = aggregateTime;
      mNumInvoked = timesInvoked;
    }

    LoggingInfo(String encoded) {
      String[] parts = encoded.split(", ");
      mAggregateTime = Long.decode(parts[0]);
      mNumInvoked = Integer.decode(parts[1]);
    }

    @Override
    public String toString() {
      return mAggregateTime.toString() + "," + mNumInvoked.toString();
    }

    public LoggingInfo increment(long addToTime) {
      Preconditions.checkArgument(addToTime >= 0);
      mAggregateTime += addToTime;
      mNumInvoked += 1;
      return this;
    }

    public LoggingInfo increment(long addToTime, int addToInvoked) {
      Preconditions.checkArgument(addToTime >=0 && addToInvoked >= 0);
      mAggregateTime += addToTime;
      mNumInvoked += addToInvoked;
      return this;
    }

    public Float perCallTime() {
      return mAggregateTime/mNumInvoked.floatValue();
    }
  }

  protected LogTimerAspect() {
    Runtime.getRuntime( ).addShutdownHook(new LogWriterThread());
    mSignatureTimeMap = new HashMap<String, LoggingInfo>();
    mPid = ManagementFactory.getRuntimeMXBean().getName();
  }



  @Pointcut("execution(* org.kiji.schema.KijiCellDecoder.*(..))")
  protected void profile(){
  }

  @Around("profile()")
  public Object aroundProfileMethods(final ProceedingJoinPoint thisJoinPoint) throws Throwable {
    Logger LOG = LoggerFactory.getLogger(thisJoinPoint.getTarget().getClass());
    final long start, end;
    start = System.nanoTime();
    Object returnanswer = thisJoinPoint.proceed();
    end = System.nanoTime();
    System.out.println("calledaspect");
    String funcSig = thisJoinPoint.getSignature().toLongString();
    if (!mSignatureTimeMap.containsKey(funcSig)) {
      mSignatureTimeMap.put(funcSig, new LoggingInfo());
    } else {
      mSignatureTimeMap.put(funcSig, mSignatureTimeMap.get(funcSig).increment(end-start));
    }
    return returnanswer;
  }
}