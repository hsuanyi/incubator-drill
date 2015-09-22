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
package org.apache.drill.exec;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestLimitZeroPushDown extends BaseTestQuery {
  @Test
  public void testJoinLimitZero() throws Exception {
    final String query1 = "select * from cp.`tpch/region.parquet` a, cp.`tpch/nation.parquet` b \n" +
        "where 1 = 0 and a.r_regionkey = b.n_nationkey";

    final String query2 = "select * from cp.`tpch/region.parquet` a inner join cp.`tpch/nation.parquet` b \n" +
        "on 1 = 0 and a.r_regionkey = b.n_nationkey";

    final String query3 = "select * from cp.`tpch/region.parquet` a inner join cp.`tpch/nation.parquet` b \n" +
        "on 1 = 0 \n" +
        "where a.r_regionkey = b.n_nationkey";

    final String query4 = "select * from cp.`tpch/region.parquet` a inner join cp.`tpch/nation.parquet` b \n" +
        "on a.r_regionkey = b.n_nationkey \n" +
        "where 1 = 0";

    test("explain plan for " + query1);
    test("explain plan for " + query2);
    test("explain plan for " + query3);
    test("explain plan for " + query4);
  }

  @Test
  public void testUnionAllLimitZero() throws Exception {
    final String query = "select * from \n" +
        "(select a from cp.`tpch/region.parquet`) union all (select a from cp.`tpch/region.parquet`) \n" +
        "limit 0";

    test(query);
  }
}
