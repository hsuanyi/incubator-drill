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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

public class DrillCalciteSqlFunctionWrapper extends SqlFunction {
  public final SqlFunction wrappedFunction;
  private SqlOperandTypeChecker operandTypeChecker = new Checker();

  public DrillCalciteSqlFunctionWrapper(final SqlFunction wrappedFunction) {
    super(wrappedFunction.getName(),
        wrappedFunction.getSqlIdentifier(),
        wrappedFunction.getKind(),
        wrappedFunction.getReturnTypeInference(),
        wrappedFunction.getOperandTypeInference(),
        wrappedFunction.getOperandTypeChecker(),
        wrappedFunction.getParamTypes(),
        wrappedFunction.getFunctionType());

    this.wrappedFunction = wrappedFunction;
  }

  public final SqlFunction getWrappedSqlFunction() {
    return wrappedFunction;
  }

  @Override
  public boolean validRexOperands(int count, boolean fail) {
    return true;
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
    return wrappedFunction.getAllowedSignatures(opNameToUse);
  }

  @Override
  public SqlOperandTypeInference getOperandTypeInference() {
    return wrappedFunction.getOperandTypeInference();
  }

  @Override
  public boolean isAggregator() {
    return wrappedFunction.isAggregator();
  }

  @Override
  public boolean requiresOrder() {
    return wrappedFunction.requiresOrder();
  }

  @Override
  public boolean allowsFraming() {
    return wrappedFunction.allowsFraming();
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return wrappedFunction.getReturnTypeInference();
  }

  @Override
  public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    return wrappedFunction.getMonotonicity(call);
  }

  @Override
  public boolean isDeterministic() {
    return wrappedFunction.isDeterministic();
  }

  @Override
  public boolean isDynamicFunction() {
    return wrappedFunction.isDynamicFunction();
  }

  @Override
  public boolean requiresDecimalExpansion() {
    return wrappedFunction.requiresDecimalExpansion();
  }

  @Override
  public boolean argumentMustBeScalar(int ordinal) {
    return wrappedFunction.argumentMustBeScalar(ordinal);
  }

  @Override
  public SqlOperandTypeChecker getOperandTypeChecker() {
    return operandTypeChecker;
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return operandTypeChecker.getOperandCountRange();
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return wrappedFunction.inferReturnType(opBinding);
  }

  @Override
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return true;
  }

  @Override
  public SqlSyntax getSyntax() {
    return wrappedFunction.getSyntax();
  }

  @Override
  public List<String> getParamNames() {
    return wrappedFunction.getParamNames();
  }

  @Override
  public String getSignatureTemplate(final int operandsCount) {
    return wrappedFunction.getSignatureTemplate(operandsCount);
  }

  @Override
  public boolean isQuantifierAllowed() {
    return wrappedFunction.isQuantifierAllowed();
  }

  @Override
  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    return wrappedFunction.deriveType(validator,
        scope,
        call);
  }

  @Override
  public <R> R acceptCall(SqlVisitor<R> visitor, SqlCall call) {
    R node = null;
    for (SqlNode operand : call.getOperandList()) {
      if (operand == null) {
        continue;
      }
      node = operand.accept(visitor);
    }
    return node;
  }
}
