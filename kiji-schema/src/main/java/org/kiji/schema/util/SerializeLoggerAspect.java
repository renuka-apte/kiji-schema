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
import java.util.HashMap;

import org.aspectj.lang.Aspects;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import sun.management.ManagementFactory;

@Aspect
public class SerializeLoggerAspect {
  private String mPid;
  private LogTimerAspect mLogTimerAspect;

  protected SerializeLoggerAspect() {
    mPid = ManagementFactory.getRuntimeMXBean().getName();
    if (Aspects.hasAspect(LogTimerAspect.class)) {
      mLogTimerAspect = Aspects.aspectOf(LogTimerAspect.class);
    } else {
      throw new RuntimeException("Log Timer aspect not found!");
    }
  }

  @Pointcut("execution(* org.kiji.schema.tools.KijiTool.toolMain(..))")
  protected void writeResultsLocal() {
  }

  @After("writeResultsLocal()")
  public void afterToolMain(final JoinPoint thisJoinPoint) {
    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter("/tmp/logfile", true);
      fileWriter.write(mPid + "\n");
      HashMap<String, LogTimerAspect.LoggingInfo> signatureTimeMap =
          mLogTimerAspect.getSignatureTimeMap();
      for (String key: signatureTimeMap.keySet()) {
        LogTimerAspect.LoggingInfo loggingInfo = signatureTimeMap.get(key);
        fileWriter.write("In tool: " + thisJoinPoint.getSignature().toLongString()
            + ", Function: " + key + ", " + loggingInfo.toString() + ", "
            + loggingInfo.perCallTime().toString() + "\n");
      }
      fileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
