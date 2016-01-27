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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestFunctionsWithTypeExpoQueries extends BaseTestQuery {
  @Test
  public void testMinus() throws Exception {
    final String query = "select r_regionkey - r_regionkey \n" +
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
    final String query = "select extract(second from time '02:30:45.100') as col from cp.`tpch/region.parquet` limit 0";

    testBuilder()
        .sqlQuery(query)
            .csvBaselineFile("testframework/testUnionAllQueries/q18_1.tsv")

            .baselineTypes(TypeProtos.MinorType.VARCHAR)


            .ordered()
        .baselineColumns("col")
        .baselineValues(45.1)
        .baselineValues(45.1)
        .baselineValues(45.1)
        .baselineValues(45.1)
        .baselineValues(45.1)
        .build()
        .run();
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
        .baselineValues(1)
        .build()
        .run();
  }

  @Test
  public void testExtract() throws Exception {
    final String query = "select extract(second from col1) \n" +
        "from cp.`employee.json` limit 0";

    test(query);
  }

  @Test
  public void testFlatten() throws Exception {
    test("explain plan for select flatten(a) from cp.`tpch/region.parquet`;");
  }

  @Test
  public void tesIsNull() throws Exception {
    final String query = "select r_name is null as col from cp.`tpch/region.parquet` limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder();
    builder.setMinorType(TypeProtos.MinorType.BIGINT);
    builder.setMode(TypeProtos.DataMode.OPTIONAL);
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), builder.build()));

    // Validate the result
    testBuilder()
        .sqlQuery(query)
        .unOrdered()
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testAnyTypeMinusTimestamp() throws Exception {
    final String query = "select c_timestamp + interval '30-11' year to month as col1 from cp.`tpch/region.parquet` \n" +
        "where (c_timestamp - to_timestamp('2014-02-13 17:32:33','YYYY-MM-dd HH:mm:ss') < interval '5 15:40:50' day to second)";
    test("explain plan for " + query);
  }

  @Test
  public void testCast() throws Exception {
    final String query = "select cast(a as Integer) as col1 from cp.`tpch/region.parquet`";
    test("explain plan for " + query);
  }

  @Test
  public void t() throws Exception {
    test("SELECT (CASE WHEN (`campaign_sales`.`eml_triggered` = 1) THEN 'Triggered' WHEN (`campaign_sales`.`eml_triggered` = 0) THEN 'Non-Triggered' ELSE 'Others' END) AS `Calculation_BDEBCAIBADBDDACJ`,\n" +
            "  `campaign_sales`.`cmp_campaign_name` AS `cmp_campaign_name`,\n" +
            "  SUM((CASE WHEN (NOT (CAST(`campaign_sales`.`eml_clicked_on` AS DATE) IS NULL)) THEN 1 ELSE 0 END)) AS `TEMP_Calculation_BCABCAIBCFFBFFCF__CFGGHCGIB__A_`,\n" +
            "  SUM((CASE WHEN (CAST(`campaign_sales`.`eml_opened_on` AS DATE) IS NULL) THEN 0 ELSE 1 END)) AS `TEMP_Calculation_BCABCAIBCFFBFFCF__HBIHJJCCA__A_`,\n" +
            "  COUNT(DISTINCT `campaign_sales`.`pro_redeemed`) AS `TEMP_Calculation_CAEAICFBGBECIAHE__CBFDAFCEGI__A_`,\n" +
            "  COUNT(DISTINCT `campaign_sales`.`eml_mailing_audience_id`) AS `TEMP_Calculation_DGBBCAIBAAIEAIEE__CCJFFEHICH__A_`,\n" +
            "  COUNT(DISTINCT (CASE WHEN ((CASE WHEN (CAST(`campaign_sales`.`eml_opened_on` AS DATE) IS NULL) THEN 0 ELSE 1 END) = 1) THEN `campaign_sales`.`eml_mailing_audience_id` ELSE 0 END)) AS `TEMP_Calculation_GABBCAIBBCIECEBJ__ECHHFFDGJA__A_`,\n" +
            "  SUM((`campaign_sales`.`pos_comps` + `campaign_sales`.`pos_promos`)) AS `sum_Calculation_GGJAICFBFDDBHHFD_ok`,\n" +
            "  SUM(`campaign_sales`.`pos_netsales`) AS `sum_pos_netsales_ok`,\n" +
            "  COUNT(DISTINCT `campaign_sales`.`pay_payment_id`) AS `usr_Calculation_EDGBCAIBDAHECIGD_ok`\n" +
            "FROM cp.`tpch/region.parquet` `campaign_sales`\n" +
            "WHERE ((CASE WHEN ((CAST(`campaign_sales`.`eml_sent_on` AS DATE) >= {d '2015-01-01'}) AND (CAST(`campaign_sales`.`eml_sent_on` AS DATE) <= {d '2015-12-31'})) THEN 1 ELSE 0 END) = 1)\n" +
            "GROUP BY (CASE WHEN (`campaign_sales`.`eml_triggered` = 1) THEN 'Triggered' WHEN (`campaign_sales`.`eml_triggered` = 0) THEN 'Non-Triggered' ELSE 'Others' END),\n" +
            "  `campaign_sales`.`cmp_campaign_name`");
  }
}
