/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.schema.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;

import org.kiji.annotations.ApiAudience;

/** Factory for HTableInterface that creates concrete HTable instances. */
@ApiAudience.Private
public final class CachedHTableInterfaceFactory implements HTableInterfaceFactory {
  /** Singleton. */
  private static final HTableInterfaceFactory DEFAULT = new CachedHTableInterfaceFactory();

  /** @return the default factory singleton. */
  public static HTableInterfaceFactory get() {
    return DEFAULT;
  }

  /** Disallow new instances, enforce singleton. */
  private CachedHTableInterfaceFactory() {
  }

  /** {@inheritDoc} */
  @Override
  public HTableInterface create(Configuration conf, String hbaseTableName) throws IOException {
    return new CachedHTable(conf, hbaseTableName);
  }
}
