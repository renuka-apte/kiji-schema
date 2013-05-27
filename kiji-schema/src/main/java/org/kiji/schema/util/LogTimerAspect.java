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

import java.util.HashMap;

import com.google.common.base.Preconditions;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;

/**
 * Aspect to measure encoding and decoding of Kiji cells and time spent
 * accessing the meta table.
 */
@Aspect
public class LogTimerAspect {
  /**
   * The HashMap containing information about a function call, the aggregate
   * time spent within this function and the number of times it was invoked.
   */
  private HashMap<String, LoggingInfo> mSignatureTimeMap = null;

  /**
   * The LoggingInfo class stores the information such as aggregate
   * time taken by all invocations of a function in nanoseconds and how
   * many times it was invoked.
   */
  class LoggingInfo {
    private Long mAggregateTime;
    private Integer mNumInvoked;

    /**
     * Default constructor.
     */
    LoggingInfo() {
      mAggregateTime = 0L;
      mNumInvoked = 0;
    }

    /**
     * Constructor for LoggingInfo.
     *
     * @param aggregateTime nanoseconds spent so far in a function call.
     * @param timesInvoked number of times function was invoked so far.
     */
    LoggingInfo(long aggregateTime, int timesInvoked) {
      mAggregateTime = aggregateTime;
      mNumInvoked = timesInvoked;
    }

    /**
     * Decode a string into {@link LoggingInfo}.
     *
     * @param encoded the string to be decoded.
     */
    LoggingInfo(String encoded) {
      String[] parts = encoded.split(", ");
      mAggregateTime = Long.decode(parts[0]);
      mNumInvoked = Integer.decode(parts[1]);
    }

    /**
     * Encode the contents of this instance into a string.
     *
     * @return encoded string for the contents.
     */
    @Override
    public String toString() {
      return mAggregateTime.toString() + "," + mNumInvoked.toString();
    }

    /**
     * Add to the aggregate time of a function call. By default this means
     * we are adding 1 to the number of method invocations.
     *
     * @param addToTime nanoseconds to add to the aggregate time spent in a function.
     * @return this LoggingInfo instance.
     */
    public LoggingInfo increment(long addToTime) {
      Preconditions.checkArgument(addToTime >= 0);
      mAggregateTime += addToTime;
      mNumInvoked += 1;
      return this;
    }

    /**
     * Add to the aggregate time spent in a function and number of times it is invoked.
     *
     * @param addToTime nanoseconds to add to the total time spent in a function.
     * @param addToInvoked add to the number of times function was invoked.
     * @return this LoggingInfo instance.
     */
    public LoggingInfo increment(long addToTime, int addToInvoked) {
      Preconditions.checkArgument(addToTime >= 0 && addToInvoked >= 0);
      mAggregateTime += addToTime;
      mNumInvoked += addToInvoked;
      return this;
    }

    /**
     * Get time spent per function invocation.
     *
     * @return the average time taken per call in nanoseconds.
     */
    public Float perCallTime() {
      return mAggregateTime / mNumInvoked.floatValue();
    }
  }

  /**
   * Default constructor.
   */
  protected LogTimerAspect() {
    mSignatureTimeMap = new HashMap<String, LoggingInfo>();
  }

  /**
   * Get the HashMap of functions to information stored about them.
   *
   * @return HashMap containing function calls and time spent in them.
   */
  public HashMap<String, LoggingInfo> getSignatureTimeMap() {
    return mSignatureTimeMap;
  }

  /**
   * Pointcut for Kiji encoder and decoder.
   */
  @Pointcut("execution(* org.kiji.schema.KijiCellDecoder.*(..)) || "
      + "execution(* org.kiji.schema.KijiCellEncoder.*(..))")
  protected void profile(){
  }

  /**
   * Advice around functions that match PointCut "profile".
   *
   * @param thisJoinPoint The JoinPoint that matched the pointcut.
   * @return Object returned by function which matched PointCut "profile".
   * @throws Throwable if there is an exception in the function the advice surrounds.
   */
  @Around("profile()")
  public Object aroundProfileMethods(final ProceedingJoinPoint thisJoinPoint) throws Throwable {
    final long start, end;
    start = System.nanoTime();
    Object returnanswer = thisJoinPoint.proceed();
    end = System.nanoTime();
    System.out.println("calledaspect");
    String funcSig = thisJoinPoint.getSignature().toLongString();
    if (!mSignatureTimeMap.containsKey(funcSig)) {
      mSignatureTimeMap.put(funcSig, new LoggingInfo(end - start, 1));
    } else {
      mSignatureTimeMap.put(funcSig, mSignatureTimeMap.get(funcSig).increment(end - start));
    }
    return returnanswer;
  }
}
