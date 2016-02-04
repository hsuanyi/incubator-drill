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
import org.apache.calcite.rel.type.RelDataTypeFactory;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class DrillCalciteSqlOperatorWrapper extends SqlOperator implements DrillCalciteSqlWrapper {
  public final SqlOperator sqlOperator;
  private SqlOperandTypeChecker operandTypeChecker = new Checker();

  public DrillCalciteSqlOperatorWrapper(SqlOperator sqlOperator) {
    super(
        sqlOperator.getName(),
        sqlOperator.getKind(),
        sqlOperator.getLeftPrec(),
        sqlOperator.getRightPrec(),
        sqlOperator.getReturnTypeInference(),
        sqlOperator.getOperandTypeInference(),
        sqlOperator.getOperandTypeChecker());
    this.sqlOperator = sqlOperator;
  }

  @Override
  public SqlOperator getOperator() {
    return sqlOperator;
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
        return sqlOperator.getSyntax();
    }

  @Override
  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    return sqlOperator.createCall(functionQualifier, pos, operands);
  }

    @Override
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
        return sqlOperator.rewriteCall(validator, call);
    }

    @Override
    public void unparse(
            SqlWriter writer,
            SqlCall call,
            int leftPrec,
            int rightPrec) {
      sqlOperator.unparse(writer, call, leftPrec, rightPrec);
    }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    if(opBinding.getOperator().getName().toUpperCase().equals("ROW")) {
      SqlTypeName sqlTypeName = opBinding.getOperandType(0).getSqlTypeName();
      if(sqlTypeName == SqlTypeName.INTEGER) {
        final RelDataType type = opBinding.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        return type;
      } else if(sqlTypeName == SqlTypeName.FLOAT) {
        final RelDataType type = opBinding.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
        return type;
      }
    }

    return sqlOperator.inferReturnType(opBinding);
  }

  @Override
  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    return sqlOperator.deriveType(validator,
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
    return sqlOperator.getSignatureTemplate(operandsCount);
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
        return sqlOperator.getAllowedSignatures(opNameToUse);
    }

  @Override
  public SqlOperandTypeInference getOperandTypeInference() {
        return sqlOperator.getOperandTypeInference();
    }

  @Override
  public boolean isAggregator() {
    return sqlOperator.isAggregator();
  }

  @Override
  public boolean requiresOrder() {
        return sqlOperator.requiresOrder();
    }

  @Override
  public boolean allowsFraming() {
        return sqlOperator.allowsFraming();
    }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
        return sqlOperator.getReturnTypeInference();
    }

  @Override
  public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
        return sqlOperator.getMonotonicity(call);
    }

  @Override
  public boolean isDeterministic() {
        return sqlOperator.isDeterministic();
    }

  @Override
  public boolean isDynamicFunction() {
        return sqlOperator.isDynamicFunction();
    }

  @Override
  public boolean requiresDecimalExpansion() {
        return sqlOperator.requiresDecimalExpansion();
    }

  @Override
  public boolean argumentMustBeScalar(int ordinal) {
        return sqlOperator.argumentMustBeScalar(ordinal);
    }
}