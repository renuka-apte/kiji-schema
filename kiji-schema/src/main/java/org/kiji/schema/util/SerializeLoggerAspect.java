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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import org.aspectj.lang.Aspects;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;

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
   * Advice for running after any functions that match PointCut "writeResultsLocal".
   *
   * @param thisJoinPoint The joinpoint that matched the pointcut.
   */
  @AfterReturning("call(* org.kiji.schema.tools.KijiTool.toolMain(..))")
  public void afterToolMain(final JoinPoint thisJoinPoint) {
    try {
      FileOutputStream fos = new FileOutputStream("/tmp/logfile", true);
      try {
        OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");
        try {
          out.write(mPid + "\n");
          HashMap<String, LoggingInfo> signatureTimeMap =
              mLogTimerAspect.getSignatureTimeMap();
          for (Map.Entry<String, LoggingInfo> entrySet: signatureTimeMap.entrySet()) {
            out.write("In tool: " + thisJoinPoint.getSignature().toLongString()
                + ", Function: " + entrySet.getKey() + ", " + entrySet.getValue().toString() + ", "
                + entrySet.getValue().perCallTime().toString() + "\n");
          }
        } finally {
          out.close();
        }
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      } finally {
        fos.close();
      }
    } catch (IOException fnf) {
      fnf.printStackTrace();
    }
  }
}
