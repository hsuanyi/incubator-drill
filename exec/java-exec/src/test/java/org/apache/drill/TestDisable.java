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
import org.apache.drill.exec.rpc.RpcException;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestDisable extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

  @Test(expected = RpcException.class) // see DRILL-1877
  public void testDisabledConcatDoublePipeAtPlanning() throws Exception {
    test("select * from (select * from cp.`tpch/lineitem.parquet` where (cast(l_orderkey as varchar) || cast(l_partkey as varchar)) = `exe`);");
  }

  @Test(expected = RpcException.class) // see DRILL-1921
  public void testDisabledUnion() throws Exception {
    test("(select n_name as name from cp.`tpch/nation.parquet`) UNION (select r_name as name from cp.`tpch/region.parquet`)");
  }

  @Test(expected = RpcException.class) // see DRILL-1921
  public void testDisabledIntersect() throws Exception {
    test("(select n_name as name from cp.`tpch/nation.parquet`) INTERSECT (select r_name as name from cp.`tpch/region.parquet`)");
  }

  @Test(expected = RpcException.class) // see DRILL-1921
  public void testDisabledExcept() throws Exception {
    test("(select n_name as name from cp.`tpch/nation.parquet`) EXCEPT (select r_name as name from cp.`tpch/region.parquet`)");
  }

  @Test(expected = RpcException.class) // see DRILL-1921
  public void testDisabledNaturalJoin() throws Exception {
    test("select * from cp.`tpch/nation.parquet` NATURAL JOIN cp.`tpch/region.parquet`");
  }

  @Test(expected = RpcException.class) // see DRILL-1921
  public void testDisabledCrossJoin() throws Exception {
    test("select * from cp.`tpch/nation.parquet` CROSS JOIN cp.`tpch/region.parquet`");
  }

  @Test(expected = RpcException.class) // see DRILL-1959
  public void testDisabledCastTINYINT() throws Exception {
    test("select cast(n_name as tinyint) from cp.`tpch/nation.parquet`;");
  }

  @Test(expected = RpcException.class) // see DRILL-1959
  public void testDisabledCastSMALLINT() throws Exception {
    test("select cast(n_name as smallint) from cp.`tpch/nation.parquet`;");
  }

  @Test(expected = RpcException.class) // see DRILL-1959
  public void testDisabledCastREAL() throws Exception {
    test("select cast(n_name as real) from cp.`tpch/nation.parquet`;");
  }

  @Test(expected = RpcException.class) // see DRILL-2016
  public void testDisabledCastMultiset() throws Exception {
    test("select cast(n_name as multiset) from cp.`tpch/nation.parquet`;");
  }

  @Test(expected = RpcException.class) // see DRILL-1936
  public void testDisabled() throws Exception {
    test("select n_name from cp.`tpch/nation.parquet` " +
         "where n_nationkey = " +
         "(select r_regionkey " +
         "from cp.`tpch/region.parquet` " +
         "where r_regionkey = 1)");
  }
}