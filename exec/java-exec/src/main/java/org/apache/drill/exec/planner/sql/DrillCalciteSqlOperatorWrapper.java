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
import org.apache.calcite.sql.SqlKind;
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
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class DrillCalciteSqlOperatorWrapper extends SqlOperator {
  public final SqlOperator wrappedOperator;
  private SqlOperandTypeChecker operandTypeChecker = new Checker();

  public DrillCalciteSqlOperatorWrapper(SqlOperator wrappedOperator) {
    super(
        wrappedOperator.getName(),
        wrappedOperator.getKind(),
        wrappedOperator.getLeftPrec(),
        wrappedOperator.getRightPrec(),
        wrappedOperator.getReturnTypeInference(),
        wrappedOperator.getOperandTypeInference(),
        wrappedOperator.getOperandTypeChecker());
    this.wrappedOperator = wrappedOperator;
  }

  public final SqlOperator getWrappedSqlOperator() {
    return wrappedOperator;
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
  public SqlSyntax getSyntax() {
        return wrappedOperator.getSyntax();
    }

    @Override
    public SqlCall createCall(
            SqlLiteral functionQualifier,
            SqlParserPos pos,
            SqlNode... operands) {
        return wrappedOperator.createCall(functionQualifier, pos, operands);
    }

    @Override
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
        return wrappedOperator.rewriteCall(validator, call);
    }

    @Override
    public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
        wrappedOperator.unparse(writer, call, leftPrec, rightPrec);
    }

  @Override
  public RelDataType inferReturnType(
            SqlOperatorBinding opBinding) {
    return wrappedOperator.inferReturnType(opBinding);
  }

  @Override
  public RelDataType deriveType(
            SqlValidator validator,
            SqlValidatorScope scope,
            SqlCall call) {
        return wrappedOperator.deriveType(validator,
                scope,
                call);
  }

  @Override
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return true;
  }

  @Override
  public boolean validRexOperands(int count, boolean fail) {
    return true;
  }

  @Override
  public String getSignatureTemplate(final int operandsCount) {
    return wrappedOperator.getSignatureTemplate(operandsCount);
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
        return wrappedOperator.getAllowedSignatures(opNameToUse);
    }

  @Override
  public SqlOperandTypeInference getOperandTypeInference() {
        return wrappedOperator.getOperandTypeInference();
    }

  @Override
  public boolean isAggregator() {
    return wrappedOperator.isAggregator();
  }

  @Override
  public boolean requiresOrder() {
        return wrappedOperator.requiresOrder();
    }

  @Override
  public boolean allowsFraming() {
        return wrappedOperator.allowsFraming();
    }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
        return wrappedOperator.getReturnTypeInference();
    }

  @Override
  public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return wrappedOperator.getMonotonicity(call);
    }

  @Override
  public boolean isDeterministic() {
        return wrappedOperator.isDeterministic();
    }

  @Override
  public boolean isDynamicFunction() {
        return wrappedOperator.isDynamicFunction();
    }

  @Override
  public boolean requiresDecimalExpansion() {
        return wrappedOperator.requiresDecimalExpansion();
    }

  @Override
  public boolean argumentMustBeScalar(int ordinal) {
        return wrappedOperator.argumentMustBeScalar(ordinal);
    }
}