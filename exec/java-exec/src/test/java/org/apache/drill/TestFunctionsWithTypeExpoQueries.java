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

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

public class TestFunctionsWithTypeExpoQueries extends BaseTestQuery {
  @Test
  public void test() throws Exception {
    final String query = "select r_regionkey + r_regionkey \n" +
            "from cp.`tpch/region.parquet`";
   test(query);
  }

  @Test
  public void testCastVarbinaryToInt() throws Exception {
    test("explain plan for select cast(a as int) \n" +
      "from cp.`tpch/region.parquet`");
  }

  @Test
  public void testConcatWithMoreThanTwoArgs() throws Exception {
    final String query = "select concat(r_name, r_name, r_name) as col \n" +
        "from cp.`tpch/region.parquet`";
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .csvBaselineFile("testframework/testFunctionsWithTypeExpoQueries/testConcatWithMoreThanTwoArgs.tsv")
        .baselineTypes(TypeProtos.MinorType.VARCHAR)
        .baselineColumns("col")
        .build()
        .run();
  }

  @Test
  public void testTrimOnlyOneArg() throws Exception {
    final String query1 = "SELECT ltrim('drill') as col FROM (VALUES(1))";
    final String query2 = "SELECT rtrim('drill') as col FROM (VALUES(1))";
    final String query3 = "SELECT btrim('drill') as col FROM (VALUES(1))";

    testBuilder()
        .sqlQuery(query1)
        .ordered()
        .baselineColumns("col")
        .baselineValues("drill")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .ordered()
        .baselineColumns("col")
        .baselineValues("drill")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .ordered()
        .baselineColumns("col")
        .baselineValues("drill")
        .build()
        .run();
  }

  @Test
  public void testLengthWithVariArg() throws Exception {
    final String query1 = "SELECT length('drill', 'utf8') as col FROM (VALUES(1))";
    final String query2 = "SELECT length('drill') as col FROM (VALUES(1))";

    testBuilder()
        .sqlQuery(query1)
        .ordered()
        .baselineColumns("col")
        .baselineValues(5l)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .ordered()
        .baselineColumns("col")
        .baselineValues(5l)
        .build()
        .run();
  }

  @Test
  public void testPadWithTwoArg() throws Exception {
    final String query1 = "SELECT rpad('drill', 1) as col FROM (VALUES(1))";
    final String query2 = "SELECT lpad('drill', 1) as col FROM (VALUES(1))";

    testBuilder()
        .sqlQuery(query1)
        .ordered()
        .baselineColumns("col")
        .baselineValues("d")
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .ordered()
        .baselineColumns("col")
        .baselineValues("d")
        .build()
        .run();
  }

  /**
   * In the following query, the extract function would be borrowed from Calcite,
   * which asserts the return type as be BIG-INT
   */
  @Test
  public void testExtractSecond() throws Exception {
    test("select extract(second from time '02:30:45.100') from cp.`tpch/region.parquet`");
  }

  @Test
  public void testMetaDataExposeType() throws Exception {
    final String root = FileUtils.getResourceAsFile("/typeExposure/metadata_caching").toURI().toString();
    final String query = String.format("select count(*) as col \n" +
        "from dfs_test.`%s` \n" +
        "where concat(a, 'asdf') = 'asdf'", root);

    // Validate the plan
    final String[] expectedPlan = {"Scan.*a.parquet.*numFiles=1"};
    final String[] excludedPlan = {"Filter"};
    PlanTestBase.testPlanMatchingPatterns(query, expectedPlan, excludedPlan);

    // Validate the result
    testBuilder()
        .sqlQuery(query)
        .ordered()
        .baselineColumns("col")
        .baselineValues(1l)
        .build()
        .run();
  }

  @Test
  public void testMinusDate() throws Exception {
    test("explain plan for select * \n" +
        "from cp.`employee.json` \n" +
        "where to_timestamp(3) < interval '5 15:40:50' day to second");
  }

  @Test
  public void testZ() throws Exception {
    final String query = "select extract(second from cast(col1 as interval day)) \n" +
        "from cp.`employee.json`";

    test(query);
  }

  @Test
  public void testDrillOptiq() throws Exception {
    test("select NOT a from cp.`tpch/region.parquet`;");
  }
}
