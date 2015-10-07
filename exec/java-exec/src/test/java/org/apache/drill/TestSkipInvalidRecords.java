/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill;

import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.ClassTransformer;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.junit.Test;

public class TestSkipInvalidRecords extends BaseTestQuery {
  @Test
  public void testCastFailAtSomeRecords_Project() throws Exception {
    String root = FileUtils.getResourceAsFile("/testSkipInvalidRecords/testCastFailAtSomeRecords.csv").toURI().toString();
    String query = String.format("select cast(columns[0] as integer) c0, cast(columns[1] as integer) c1 \n" +
        "from dfs_test.`%s`", root);
    //test("alter session set `exec.enable_skip_invalid_record` = true");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1")
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .baselineValues(4, 4)
        .build()
        .run();
  }

  @Test
  public void testCastFailAtSomeRecords_Filter() throws Exception {
    String root = FileUtils.getResourceAsFile("/testSkipInvalidRecords/testCastFailAtSomeRecords.csv").toURI().toString();
    String query = String.format("select cast(columns[0] as integer) as c0, cast(columns[1] as integer) as c1 \n" +
            "from dfs_test.`%s` \n" +
            "where columns[0] > 1", root);
  //  test("alter session set `exec.enable_skip_invalid_record` = true");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1")
        .baselineValues(2, 2)
        .baselineValues(3, 3)
        .baselineValues(4, 4)
        .build()
        .run();
  }

  @Test
  public void testCTAS_CastFailAtSomeRecords_Project() throws Exception {
        String root = FileUtils.getResourceAsFile("/testSkipInvalidRecords/testCastFailAtSomeRecords.csv").toURI().toString();
        String query = String.format("select cast(columns[0] as integer) c0, cast(columns[1] as integer) c1 \n" +
                "from dfs_test.`%s`", root);
      //test("alter session set `exec.enable_skip_invalid_record` = true");
      //test("alter session set `exec.skip_invalid_record_threshold` = 100");

      testBuilder()
                .sqlQuery(query)
                .unOrdered()
                .baselineColumns("c0", "c1")
                .baselineValues(2, 2)
                .baselineValues(3, 3)
                .baselineValues(4, 4)
                .build()
                .run();
  }

  @Test
  public void testParquet() throws Exception {
    String query = String.format("select cast(r_name as integer) from cp.`tpch/region.parquet`");

    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .baselineColumns("c0", "c1")
        .baselineValues(2, 2)
        .build()
        .run();
  }
}
