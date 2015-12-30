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
package org.apache.drill.exec.planner.sql;

import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;

class Checker implements SqlOperandTypeChecker {
  private SqlOperandCountRange range;

  /**
   * During Calcite's validation, the SqlOperators whose number of arguments do not match with that in the given SQL query
   * will be filtered out. If the number of argument(s) is not supposed to be a criterion to filter a SqlOperator,
   * use this SqlOperandTypeChecker.
   *
   * For example, CONCAT can take arbitrary number of argument(s), so there is no reason
   * to let Calcite filter CONCAT simply based on the number of argument(s).
   */
  public Checker() {
    range = SqlOperandCountRanges.any();
  }

  public Checker(int size) {
    range = new FixedRange(size);
  }

  public Checker(int min, int max) {
    assert min <= max;
    range = SqlOperandCountRanges.between(min, max);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    return true;
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return range;
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return opName + "(Drill - Opaque)";
  }

  @Override
  public Consistency getConsistency() {
    return Consistency.NONE;
  }

  @Override
  public boolean isOptional(int i) {
    return false;
  }

}