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
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.exec.work.foreman.UnsupportedDataTypeException;
import org.apache.drill.exec.work.foreman.UnsupportedFunctionException;
import org.apache.drill.exec.work.foreman.UnsupportedRelOperatorException;
import org.junit.Test;

public class TestDisabledFunctionality extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

  @Test(expected = UnsupportedFunctionException.class)  // see DRILL-1937
  public void testDisabledExplainplanForComparisonWithNonscalarSubquery() throws Exception {
    try {
      test("explain plan for select n_name from cp.`tpch/nation.parquet` " +
           "where n_nationkey = " +
           "(select r_regionkey from cp.`tpch/region.parquet` " +
           "where r_regionkey = 1)");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class)  // see DRILL-1937
  public void testDisabledComparisonWithNonscalarSubquery() throws Exception {
    try {
      test("select n_name from cp.`tpch/nation.parquet` " +
           "where n_nationkey = " +
           "(select r_regionkey from cp.`tpch/region.parquet` " +
           "where r_regionkey = 1)");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class)  // see DRILL-1921
  public void testDisabledUnion() throws Exception {
    try {
      test("(select n_name as name from cp.`tpch/nation.parquet`) UNION (select r_name as name from cp.`tpch/region.parquet`)");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledUnionDistinct() throws Exception {
    try {
      test("(select n_name as name from cp.`tpch/nation.parquet`) UNION DISTINCT (select r_name as name from cp.`tpch/region.parquet`)");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledIntersect() throws Exception {
    try {
      test("(select n_name as name from cp.`tpch/nation.parquet`) INTERSECT (select r_name as name from cp.`tpch/region.parquet`)");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledIntersectALL() throws Exception {
    try {
      test("(select n_name as name from cp.`tpch/nation.parquet`) INTERSECT ALL (select r_name as name from cp.`tpch/region.parquet`)");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledExceptALL() throws Exception {
    try {
      test("(select n_name as name from cp.`tpch/nation.parquet`) EXCEPT ALL (select r_name as name from cp.`tpch/region.parquet`)");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledExcept() throws Exception {
    try {
      test("(select n_name as name from cp.`tpch/nation.parquet`) EXCEPT (select r_name as name from cp.`tpch/region.parquet`)");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledNaturalJoin() throws Exception {
    try {
      test("select * from cp.`tpch/nation.parquet` NATURAL JOIN cp.`tpch/region.parquet`");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedRelOperatorException.class) // see DRILL-1921
  public void testDisabledCrossJoin() throws Exception {
    try {
      test("select * from cp.`tpch/nation.parquet` CROSS JOIN cp.`tpch/region.parquet`");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedDataTypeException.class) // see DRILL-1959
  public void testDisabledCastTINYINT() throws Exception {
    try {
      test("select cast(n_name as tinyint) from cp.`tpch/nation.parquet`;");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedDataTypeException.class) // see DRILL-1959
  public void testDisabledCastSMALLINT() throws Exception {
    try {
      test("select cast(n_name as smallint) from cp.`tpch/nation.parquet`;");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedDataTypeException.class) // see DRILL-1959
  public void testDisabledCastREAL() throws Exception {
    try {
      test("select cast(n_name as real) from cp.`tpch/nation.parquet`;");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }

  @Test(expected = UnsupportedFunctionException.class) // see DRILL-2115
  public void testDisabledCardinality() throws Exception {
    try {
      test("select cardinality(employee_id) from cp.`employee.json`;");
    } catch(Exception ex) {
      SqlUnsupportedException.errorMessageToException(ex.getMessage());
      throw ex;
    }
  }
}