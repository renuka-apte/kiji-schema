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

package org.kiji.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.avro.Node;
import org.kiji.schema.avro.SchemaStorage;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.HBaseKijiRowData;
import org.kiji.schema.impl.RawEntityId;
import org.kiji.schema.layout.ColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.CellSpec;

public class TestHBaseKijiRowData extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseKijiRowData.class);

  private byte[] mHBaseFamily;
  private byte[] mHBaseQual0;
  private byte[] mHBaseQual1;
  private byte[] mHBaseQual2;
  private byte[] mHBaseQual3;
  private byte[] mHBaseNodequal0;
  private byte[] mHBaseNodequal1;
  private byte[] mHBaseEmpty;
  private byte[] mHBaseMapFamily;

  private EntityIdFactory mEntityIdFactory;

  private byte[] n(Node node) throws IOException {
    final CellSchema cellSchema = CellSchema.newBuilder()
        .setStorage(SchemaStorage.HASH)
        .setType(SchemaType.CLASS)
        .setValue(Node.SCHEMA$.getFullName())
        .build();
    final CellSpec cellSpec = new CellSpec()
        .setCellSchema(cellSchema)
        .setSchemaTable(getKiji().getSchemaTable());
    final KijiCellEncoder encoder =
        DefaultKijiCellEncoderFactory.get().create(cellSpec);
    return encoder.encode(node);
  }

  @Before
  public void setupLayout() throws Exception {
    final KijiTableLayout tableLayout = getKiji().getMetaTable()
        .updateTableLayout("table", KijiTableLayouts.getLayout(KijiTableLayouts.ROW_DATA_TEST));

    ColumnNameTranslator translator = new ColumnNameTranslator(tableLayout);
    HBaseColumnName hcolumn = translator.toHBaseColumnName(new KijiColumnName("family", "empty"));
    mHBaseFamily = hcolumn.getFamily();
    mHBaseEmpty = hcolumn.getQualifier();
    mHBaseQual0 = translator.toHBaseColumnName(new KijiColumnName("family:qual0")).getQualifier();
    mHBaseQual1 = translator.toHBaseColumnName(new KijiColumnName("family:qual1")).getQualifier();
    mHBaseQual2 = translator.toHBaseColumnName(new KijiColumnName("family:qual2")).getQualifier();
    mHBaseQual3 = translator.toHBaseColumnName(new KijiColumnName("family:qual3")).getQualifier();
    mHBaseNodequal0 = translator.toHBaseColumnName(new KijiColumnName("family:nodequal0"))
        .getQualifier();
    mHBaseNodequal1 = translator.toHBaseColumnName(new KijiColumnName("family:nodequal1"))
        .getQualifier();
    mHBaseMapFamily = translator.toHBaseColumnName(new KijiColumnName("map")).getFamily();

    mEntityIdFactory = EntityIdFactory.create(tableLayout.getDesc().getKeysFormat());
  }

  @Test
  public void testEntityId() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId foo = mEntityIdFactory.fromKijiRowKey("foo");
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseQual0,
            Bytes.toBytes("bot")));
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseQual1,
            Bytes.toBytes("car")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual0"));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(), tableLayout,
        result, getKiji().getSchemaTable());
    assertEquals(foo, input.getEntityId());
  }

  @Test
  public void testReadInts() throws IOException {
    LOG.info("start testReadInts");
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();

    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual3, 1L, i(42)));

    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family"));
    HBaseKijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    input.getMap();
    final int integer = (Integer) input.getMostRecentValue("family", "qual3");
    assertEquals(42, integer);
    LOG.info("stop testReadInts");
  }

  @Test
  public void testGetReaderSchema() throws IOException {
    Result result = new Result();
    KijiDataRequest dataRequest = new KijiDataRequest();
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");

    KijiRowData input = new HBaseKijiRowData(
        RawEntityId.fromKijiRowKey(Bytes.toBytes("row-key")), dataRequest,
        getCellDecoderFactory(), tableLayout, result, getKiji().getSchemaTable());

    assertEquals(Schema.create(Schema.Type.STRING), input.getReaderSchema("family", "empty"));
    assertEquals(Schema.create(Schema.Type.INT), input.getReaderSchema("family", "qual3"));
  }

  @Test(expected=NoSuchColumnException.class)
  public void testGetReaderSchemaNoSuchColumn() throws IOException {
    Result result = new Result();
    KijiDataRequest dataRequest = new KijiDataRequest();
    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");

    KijiRowData input = new HBaseKijiRowData(
        RawEntityId.fromKijiRowKey(Bytes.toBytes("row-key")), dataRequest,
        getCellDecoderFactory(), tableLayout, result, getKiji().getSchemaTable());

    input.getReaderSchema("this-family", "does-not-exist");
  }

  /**
   * This test was created in response to WIBI-41.  If your KijiDataRequest doesn't contain
   * one of the columns in the Result map, you used to a get a NullPointerException.
   */
  @Test
  public void testGetMap() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId foo = mEntityIdFactory.fromKijiRowKey("foo");
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseQual0,
            Bytes.toBytes("bot")));
    kvs.add(new KeyValue(foo.getHBaseRowKey(), mHBaseFamily, mHBaseEmpty,
            Bytes.toBytes("car")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    // We didn't request any data, so the map should be null.
    HBaseKijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.getMap().isEmpty());
  }

  @Test
  public void testReadWithMaxVersions() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            3L, e("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            2L, e("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            1L, e("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            6L, e("antelope")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            5L, e("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            4L, e("cat")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual0").withMaxVersions(1));
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual1").withMaxVersions(2));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertEquals(1, input.getValues("family", "qual0").size());
    assertEquals("apple", input.getMostRecentValue("family",  "qual0").toString());
    assertEquals(2, input.getValues("family", "qual1").size());
    assertEquals("antelope", input.getValues("family", "qual1").get(6L).toString());
    assertEquals("bear", input.getValues("family", "qual1").get(5L).toString());
  }

  @Test
  public void testTypedReadWithMaxVersions() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 3L, e("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 2L, e("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, 1L, e("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 6L, Bytes.toBytes("antelope")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 5L, Bytes.toBytes("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, 4L, Bytes.toBytes("cat")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual0").withMaxVersions(1));
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual1").withMaxVersions(2));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertEquals(1, input.getValues("family", "qual0").size());
    NavigableMap<Long, CharSequence> typedValues = input.getValues("family", "qual0");
    assertEquals("apple", typedValues.get(3L).toString());
    assertEquals(2, input.getTimestamps("family", "qual1").size());
  }

  @Test
  public void testReadWithTimeRange() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            3L, e("apple")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            2L, e("banana")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0,
            1L, e("carrot")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            5L, e("bear")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1,
            4L, e("cat")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.withTimeRange(2L, 6L);
    dataRequest.addColumn(
        new KijiDataRequest.Column("family", "qual0").withMaxVersions(1));
    dataRequest.addColumn(
        new KijiDataRequest.Column("family", "qual1").withMaxVersions(2));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertEquals(1, input.getTimestamps("family", "qual0").size());
    assertEquals("apple", input.getMostRecentValue("family", "qual0").toString());
    assertEquals(2, input.getTimestamps("family", "qual1").size());
    assertEquals("bear", input.getMostRecentValue("family", "qual1").toString());
    assertEquals("cat", input.getValue("family", "qual1", 4L).toString());
  }

  @Test
  public void testReadColumnTypes() throws IOException {
    LOG.info("start testReadColumnTypes");
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    kvs.add(new KeyValue(row0.getHBaseRowKey(), mHBaseFamily, mHBaseQual0, e("value")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual0"));
    HBaseKijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    for (String family : input.getMap().keySet()) {
      LOG.info("Family: " + family);
      for (String qual : input.getMap().get(family).keySet()) {
        LOG.info("Qualifier: " + qual);
      }
    }
    assertFalse(input.containsColumn("not-a-family"));
    assertTrue(input.containsColumn("family"));
    assertTrue(input.containsColumn("family", "qual0"));
    assertEquals("value", input.getMostRecentValue("family", "qual0").toString());
    assertEquals("value", input.getMostRecentValue("family", "qual0").toString());
    LOG.info("stop testReadColumnTypes");
  }

  @Test
  public void testReadFamilyTypes() throws IOException {
    final List<KeyValue> kvs = new ArrayList<KeyValue>();
    final EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    final byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, e("value0")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, e("value1")));
    final Result result = new Result(kvs);

    final KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    final KijiDataRequest dataRequest = new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("family", "qual0"))
        .addColumn(new KijiDataRequest.Column("family", "qual1"));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "qual0"));
    assertEquals("value0", input.getMostRecentValue("family", "qual0").toString());
    assertEquals("value0", input.getMostRecentValue("family", "qual0").toString());
    assertTrue(input.containsColumn("family", "qual1"));
    assertEquals("value1", input.getMostRecentValue("family", "qual1").toString());
    assertEquals("value1", input.getMostRecentValue("family", "qual1").toString());
    final NavigableMap<String, CharSequence> strings = input.getMostRecentValues("family");
    assertEquals(2, strings.size());
    assertEquals("value0", strings.get("qual0").toString());
    assertEquals("value1", strings.get("qual1").toString());

    final NavigableMap<String, NavigableMap<Long, CharSequence>> stringsByTime =
       input.getValues("family");
    assertEquals(2, stringsByTime.size());
    final NavigableMap<Long, CharSequence> qual0Strings = stringsByTime.get("qual0");
    assertEquals("value0", qual0Strings.get(qual0Strings.firstKey()).toString());
  }

  @Test
  public void testReadSpecificFamilyTypes() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    Node node0 = new Node();
    node0.setLabel("node0");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, n(node0)));
    Node node1 = new Node();
    node1.setLabel("node1");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal1, n(node1)));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family"));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "nodequal0"));
    assertTrue(input.containsColumn("family", "nodequal1"));
    NavigableMap<String, Node> values = input.getMostRecentValues("family");
    assertEquals(2, values.size());
    assertEquals("node0", values.get("nodequal0").getLabel().toString());
    assertEquals("node1", values.get("nodequal1").getLabel().toString());
  }

  @Test
  public void testReadSpecificTimestampTypes() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    Node node0 = new Node();
    node0.setLabel("node0");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 100L, n(node0)));
    Node node1 = new Node();
    node1.setLabel("node1");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 200L, n(node1)));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(
        new KijiDataRequest.Column("family", "nodequal0").withMaxVersions(Integer.MAX_VALUE));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "nodequal0"));
    NavigableMap<Long, Node> values = input.getValues("family", "nodequal0");
    assertNotNull(values);
    assertEquals(2, values.size());
    assertEquals("node0", values.get(100L).getLabel().toString());
    assertEquals("node1", values.get(200L).getLabel().toString());

    // Make sure they come in reverse chronological order.
    Iterator<NavigableMap.Entry<Long, Node>> iter = values.entrySet().iterator();
    assertTrue(iter.hasNext());
    assertEquals(100L, iter.next().getKey().longValue());
    assertEquals(200L, iter.next().getKey().longValue());
    assertFalse(iter.hasNext());
  }

  @Test
  public void testReadWithTimestamp() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    Node node0 = new Node();
    node0.setLabel("node0");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 100L, n(node0)));
    Node node1 = new Node();
    node1.setLabel("node1");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, 200L, n(node1)));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(
        new KijiDataRequest.Column("family", "nodequal0").withMaxVersions(Integer.MAX_VALUE));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "nodequal0"));
    assertEquals("node0",
        ((Node) input.getValue("family", "nodequal0", 100L)).getLabel().toString());
    assertEquals("node1",
        ((Node) input.getValue("family", "nodequal0", 200L)).getLabel().toString());
  }

  @Test
  public void testReadSpecificTypes() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    Node node = new Node();
    node.setLabel("foo");
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseNodequal0, n(node)));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "nodequal0"));
    KijiRowData input = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());
    assertTrue(input.containsColumn("family", "nodequal0"));
    Node actual = input.getMostRecentValue("family", "nodequal0");
    assertEquals("foo", actual.getLabel().toString());
  }

  @Test
  public void testMergePut() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, e("value0")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, e("value1")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual0"));
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual1"));
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual2"));
    HBaseKijiRowData rowData = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());

    Put put = new Put(hbaseRowKey);
    put.add(mHBaseFamily, mHBaseQual2, e("value2"));
    rowData.merge(put);

    assertTrue(rowData.containsColumn("family", "qual2"));
    NavigableMap<String, CharSequence> strings = rowData.getMostRecentValues("family");
    assertEquals(3, strings.size());
    assertEquals("value0", strings.get("qual0").toString());
    assertEquals("value1", strings.get("qual1").toString());
    assertEquals("value2", strings.get("qual2").toString());
  }

  @Test
  public void testMergeHBaseKijiRowData() throws IOException {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    EntityId row0 = mEntityIdFactory.fromKijiRowKey("row0");
    byte[] hbaseRowKey = row0.getHBaseRowKey();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual0, e("value0")));
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual1, e("value1")));
    Result result = new Result(kvs);

    KijiTableLayout tableLayout = getKiji().getMetaTable().getTableLayout("table");
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual0"));
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual1"));
    dataRequest.addColumn(new KijiDataRequest.Column("family", "qual2"));
    HBaseKijiRowData rowData = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, result, getKiji().getSchemaTable());

    kvs = new ArrayList<KeyValue>();
    kvs.add(new KeyValue(hbaseRowKey, mHBaseFamily, mHBaseQual2, e("value2")));
    Result anotherResult = new Result(kvs);
    HBaseKijiRowData anotherRowData = new HBaseKijiRowData(dataRequest, getCellDecoderFactory(),
        tableLayout, anotherResult, getKiji().getSchemaTable());
    rowData.merge(anotherRowData);

    assertTrue(rowData.containsColumn("family", "qual2"));
    NavigableMap<String, CharSequence> strings = rowData.getMostRecentValues("family");
    assertEquals(3, strings.size());
    assertEquals("value0", strings.get("qual0").toString());
    assertEquals("value1", strings.get("qual1").toString());
    assertEquals("value2", strings.get("qual2").toString());
  }
}