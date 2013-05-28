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
import java.util.Map;

import org.aspectj.lang.Aspects;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

/**
 * This aspect is invoked after the main function in any Kiji tool. It
 * accesses logging information gathered by the LogTimerAspect and
 * serializes it to a local file.
 */
@Aspect
public class SerializeLoggerAspect {
  private String mPid;
  private LogTimerAspect mLogTimerAspect;

  /**
   * Default constructor. Initializes the pid of the JVM running the tool
   * and the singleton LogTimerAspect for this JVM instance.
   */
  protected SerializeLoggerAspect() {
    mPid = ManagementFactory.getRuntimeMXBean().getName();
    if (Aspects.hasAspect(LogTimerAspect.class)) {
      mLogTimerAspect = Aspects.aspectOf(LogTimerAspect.class);
    } else {
      throw new RuntimeException("Log Timer aspect not found!");
    }
  }

  /**
   * Pointcut attached to toolMain of KijiTool.
   *
   * @param args Arguments to KijiTool.

  @Pointcut("")
  protected void writeResultsLocal(String[] args){
  }*/

  /**
   * Advice for running after any functions that match PointCut "writeResultsLocal".
   *
   * @param toolArgs Variable length of arguments passed to KijiTool.
   * @param thisJoinPoint The joinpoint that matched the pointcut.
   */
  @After("execution(* org.kiji.schema.tools.KijiTool.toolMain(String[])) && args(toolArgs)")
  public void afterToolMain(String[] toolArgs, final JoinPoint thisJoinPoint) {
    FileWriter fileWriter = null;
    try {
      fileWriter = new FileWriter("/tmp/logfile", true);
      try {
        fileWriter.write(mPid + "\n");
        HashMap<String, LogTimerAspect.LoggingInfo> signatureTimeMap =
            mLogTimerAspect.getSignatureTimeMap();
        for (Map.Entry<String, LogTimerAspect.LoggingInfo> entrySet: signatureTimeMap.entrySet()) {
          fileWriter.write("In tool: " + thisJoinPoint.getSignature().toLongString()
              + ", Function: " + entrySet.getKey() + ", " + entrySet.getValue().toString() + ", "
              + entrySet.getValue().perCallTime().toString() + "\n");
        }
      } finally {
        fileWriter.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
