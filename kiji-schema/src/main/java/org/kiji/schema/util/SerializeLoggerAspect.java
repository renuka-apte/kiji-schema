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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.aspectj.lang.Aspects;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
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
   * Pointcut to match the toolMain function of BaseTool.
   */
  @Pointcut("call(* org.kiji.schema.tools.BaseTool.toolMain(..))")
  protected void serializeTool(){
  }

  /**
   * Advice for running after any functions that match PointCut "serializeTool".
   *
   * @param thisJoinPoint The joinpoint that matched the pointcut.
   */
  @AfterReturning("serializeTool() && !cflowbelow(serializeTool())")
  public void afterToolMain(final JoinPoint thisJoinPoint) {
    try {
      Pattern p = Pattern.compile(".*\\.(\\w+)\\.toolMain.*");
      Matcher m = p.matcher(thisJoinPoint.getSignature().toLongString());
      String signature;
      if (m.matches()) {
        signature = m.group(1);
      } else {
        signature = thisJoinPoint.getSignature().toLongString();
      }
      String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmm")
          .format(Calendar.getInstance().getTime());
      String filename = "/tmp/kijistats_" + timeStamp + "_" + mPid + "_" + signature + ".csv";
      FileOutputStream fos = new FileOutputStream(filename, true);
      try {
        OutputStreamWriter out = new OutputStreamWriter(fos, "UTF-8");
        try {
          out.write("Process Id, Tool Name, Function Signature, Aggregate Time (nanoseconds), "
              + "Number of Invocations, Time per call (nanoseconds)\n");
          HashMap<String, LoggingInfo> signatureTimeMap =
              mLogTimerAspect.getSignatureTimeMap();
          for (Map.Entry<String, LoggingInfo> entrySet: signatureTimeMap.entrySet()) {
            out.write(mPid + ", " + signature + ", "
                + entrySet.getKey() + ", " + entrySet.getValue().toString() + ", "
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
