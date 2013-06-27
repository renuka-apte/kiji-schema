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
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;

import net.spy.memcached.MemcachedClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

public class CachedHTable extends HTable {
  /** Connection to cache. */
  private MemcachedClient mMemcachedClient;
  private String mMemcachedServerAddr;
  private int mMemcachedPort;

  private static final Log LOG = LogFactory.getLog(CachedHTable.class);

  public CachedHTable(Configuration conf, final String tableName) throws IOException {
    super(conf, Bytes.toBytes(tableName));
    mMemcachedServerAddr = conf.get("memcachedserver", "127.0.0.1");
    mMemcachedPort = conf.getInt("memcachedport", 11211);
    mMemcachedClient = new MemcachedClient(
        new InetSocketAddress(mMemcachedServerAddr, mMemcachedPort));
  }

  private void filterTimeRange(TimeRange timeRange, int maxVersions, TreeMap<Long, byte[]> values) {
    NavigableMap<Long, byte[]> filtered = values.subMap(timeRange.getMin(), true,
        timeRange.getMax(), true);
    if (maxVersions == Integer.MAX_VALUE) {
      return;
    }
    int count = 0;
    for (long key: filtered.descendingKeySet()) {

    }
  }
  private Map<String, TreeMap<Long, byte[]>> isCacheHit (final Get get, Get newget) {
    if (get.getFilter() != null) {
      return null;
    }

    Map<String, TreeMap<Long, byte[]>> resmap = new HashMap<String, TreeMap<Long, byte[]>>();
    Map<byte[],NavigableSet<byte[]>> familyMap = get.getFamilyMap();
    String rowKey = get.getRow().toString();
    // for each row/family/qualifier combination
    for (Map.Entry entry: familyMap.entrySet()) {
      // get the key for the cache composed of row/family/qualifier
      String cacheKey = rowKey + ":" + entry.getKey().toString() + ":" +
          entry.getValue().toString();

      // get the corresponding value for the key above
      TreeMap<Long, byte[]> cachedValue =
          (TreeMap<Long, byte[]>)mMemcachedClient.get(cacheKey);

      if (cachedValue != null) {
        // append to result after filtering on time range
        filterTimeRange(get.getTimeRange(), get.getMaxVersions(), cachedValue);
      } else {
        newget.addColumn((byte[])entry.getKey(), (byte[])entry.getValue());
      }
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result get(final Get get) throws IOException {
    Result res;
    Get newget = new Get(get.getRow());
    // Check if this exists in cache
    Map<String, TreeMap<Long, byte[]>> resmap = isCacheHit(get, newget);
    if (resmap == null) {
      return super.get(get);
    } else {
      res = super.get(newget);
      // combine cached result with result
    }
    return res;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result[] get(List<Get> gets) throws IOException {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final Delete delete) {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void delete(final List<Delete> deletes) {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final Put put) throws IOException {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(final List<Put> puts) throws IOException {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Result increment(final Increment increment) throws IOException {

  }
}
