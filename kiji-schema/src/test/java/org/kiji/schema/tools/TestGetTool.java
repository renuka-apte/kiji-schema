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

package org.kiji.schema.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;

public class TestGetTool extends KijiClientTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestGetTool.class);

  /** Horizontal ruler to delimit CLI outputs in logs. */
  private static final String RULER =
      "--------------------------------------------------------------------------------";

  /** Output of the CLI tool, as bytes. */
  private ByteArrayOutputStream mToolOutputBytes = new ByteArrayOutputStream();

  /** Output of the CLI tool, as a single string. */
  private String mToolOutputStr;

  /** Output of the CLI tool, as an array of lines. */
  private String[] mToolOutputLines;

  private int runTool(BaseTool tool, String...arguments) throws Exception {
    mToolOutputBytes.reset();
    final PrintStream pstream = new PrintStream(mToolOutputBytes);
    tool.setPrintStream(pstream);
    tool.setConf(getConf());
    try {
      LOG.info("Running tool: '{}' with parameters {}", tool.getName(), arguments);
      return tool.toolMain(Lists.newArrayList(arguments));
    } finally {
      pstream.flush();
      pstream.close();

      mToolOutputStr = Bytes.toString(mToolOutputBytes.toByteArray());
      LOG.info("Captured output for tool: '{}' with parameters {}:\n{}\n{}{}\n",
          tool.getName(), arguments,
          RULER, mToolOutputStr, RULER);
      mToolOutputLines = mToolOutputStr.split("\n");
    }
  }

  @Test
  public void testUnderspecified() throws Exception {
    final Kiji kiji = getKiji();
    final KijiURI hbaseURI = KijiURI.newBuilder(kiji.getURI()).withInstanceName(null).build();

    assertEquals(BaseTool.FAILURE, runTool(new GetTool(), hbaseURI.toString()));
    assertTrue(mToolOutputLines[0].startsWith("Specify a cluster"));
    assertEquals(BaseTool.FAILURE, runTool(new GetTool()));
    assertTrue(mToolOutputLines[0].startsWith("URI must be specified"));
    assertEquals(BaseTool.FAILURE, runTool(new GetTool(),
        hbaseURI.toString() + "instance/table",
        "--max-versions=0"));
    assertTrue(mToolOutputLines[0].startsWith("--max-versions must be positive"));
  }

  @Test
  public void testGetFromTable() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.SIMPLE);
    kiji.createTable(layout.getName(), layout);

    new InstanceBuilder(kiji)
        .withTable(layout.getName(), layout)
            .withRow("hashed")
                .withFamily("family").withQualifier("column").withValue(314L, "value")
        .build();

    final KijiTable table = kiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(), table.getURI().toString(),
          "--entity-id=hashed"));
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testGetFromTableMore() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.FOO_TEST);
    final long timestamp = 10L;
    new InstanceBuilder(kiji)
        .withTable(layout.getName(), layout)
            .withRow("gwu@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "gwu@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Garrett Wu")
            .withRow("aaron@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "aaron@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Aaron Kimball")
            .withRow("christophe@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email")
                        .withValue(timestamp, "christophe@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Christophe Bisciglia")
            .withRow("kiyan@usermail.example.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "kiyan@usermail.example.com")
                    .withQualifier("name").withValue(timestamp, "Kiyan Ahmadizadeh")
            .withRow("john.doe@gmail.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "john.doe@gmail.com")
                    .withQualifier("name").withValue(timestamp, "John Doe")
            .withRow("jane.doe@gmail.com")
                .withFamily("info")
                    .withQualifier("email").withValue(timestamp, "jane.doe@gmail.com")
                    .withQualifier("name").withValue(timestamp, "Jane Doe")
        .build();

    final KijiTable table = kiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(), table.getURI().toString(),
          "--entity-id=jane.doe@gmail.com"));
      assertEquals(5, mToolOutputLines.length);
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(),
          table.getURI() + "info:name",
          "--entity-id=hbase=hex:9decb1b27454729c38e149964ee5a0d4"
          )); // Garrett Wu
      assertEquals(3, mToolOutputLines.length);
      // TODO: Validate GetTool output
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }

  @Test
  public void testGetFormattedRKF() throws Exception {
    final Kiji kiji = getKiji();
    final KijiTableLayout layout = KijiTableLayouts.getTableLayout(KijiTableLayouts.FORMATTED_RKF);
    new InstanceBuilder(kiji)
        .withTable(layout.getName(), layout)
            .withRow("NYC", "Technology", "widget", 1, 2)
                .withFamily("family").withQualifier("column")
                    .withValue("Candaules")
            .withRow("NYC", "Technology", "widget", 1, 20)
                .withFamily("family").withQualifier("column")
                    .withValue("Croesus")
            .withRow("NYC", "Technology", "thingie", 2)
                .withFamily("family").withQualifier("column")
                    .withValue("Gyges")
            .withRow("DC", "Technology", "stuff", 123)
                .withFamily("family").withQualifier("column")
                    .withValue("Glaucon")
            .withRow("DC", "Technology", "stuff", 124, 1)
                .withFamily("family").withQualifier("column")
                    .withValue("Lydia")
        .build();

    final KijiTable table = kiji.openTable(layout.getName());
    try {
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(), table.getURI().toString(),
          "--entity-id=['NYC','Technology','widget',1,2]"
          ));
      assertEquals(3, mToolOutputLines.length);
      assertEquals(BaseTool.SUCCESS, runTool(new GetTool(), table.getURI().toString(),
          "--entity-id=['NYC','Technology','thingie',2,null]"
          ));
      assertEquals(3, mToolOutputLines.length);
    } finally {
      ResourceUtils.releaseOrLog(table);
    }
  }
}
