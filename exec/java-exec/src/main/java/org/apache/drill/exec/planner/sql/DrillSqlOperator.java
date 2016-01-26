/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.planner.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.drill.common.expression.DumbLogicalExpression;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;

public class DrillSqlOperator extends SqlFunction {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSqlOperator.class);

  public static final int MAX_VARCHAR_LENGTH = 65535;
  private static final MajorType NONE = MajorType.getDefaultInstance();
  private final boolean isDeterministic;
  private final List<DrillFuncHolder> functions;

  @Deprecated
  public DrillSqlOperator(String name, int argCount, boolean isDeterministic) {
    this(name, null, argCount, isDeterministic);
  }

  public DrillSqlOperator(String name, int argCount, MajorType returnType, boolean isDeterministic) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO),
        null,
        null,
        argCount >= 0 ? new Checker(argCount) : new Checker(),
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.isDeterministic = isDeterministic;
    this.functions = Lists.newArrayList();
  }

  public DrillSqlOperator(String name, List<DrillFuncHolder> functions, int argCount, boolean isDeterministic) {
    super(new SqlIdentifier(name, SqlParserPos.ZERO),
        null,
        null,
        argCount >= 0 ? new Checker(argCount) : new Checker(),
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    this.functions = functions;
    this.isDeterministic = isDeterministic;
  }

  @Override
  public boolean isDeterministic() {
    return isDeterministic;
  }

  private static MajorType getMajorType(RelDataType relDataType) {
    final MinorType minorType = DrillConstExecutor.getDrillTypeFromCalcite(relDataType);
    if (relDataType.isNullable()) {
      return Types.optional(minorType);
    } else {
      return Types.required(minorType);
    }
  }

  public static RelDataType getReturnType(final SqlOperatorBinding opBinding, final List<DrillFuncHolder> functions) {
    boolean allBooleanOutput = true;
    for(DrillFuncHolder function : functions) {
      if(function.getReturnType().getMinorType() != MinorType.BIT) {
        allBooleanOutput = false;
        break;
      }
    }
    if(allBooleanOutput) {
      return opBinding.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
    }

    // The following logic is just a safe play:
    // Even if any of the input arguments has ANY type,
    // it "might" still be possible to determine the return type based on other non-ANY types
    for (RelDataType type : opBinding.collectOperandTypes()) {
      if (type.getSqlTypeName() == SqlTypeName.ANY) {
        return opBinding.getTypeFactory()
                .createTypeWithNullability(opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
      }
    }

    final List<LogicalExpression> args = Lists.newArrayList();
    for(final RelDataType type : opBinding.collectOperandTypes()) {
      final MajorType majorType = getMajorType(type);
      args.add(new DumbLogicalExpression(majorType));
    }
    final FunctionCall functionCall = new FunctionCall(opBinding.getOperator().getName(), args, ExpressionPosition.UNKNOWN);
    // If the return type is VarChar,
    // set the precision as the maximum

    final FunctionResolver functionResolver = FunctionResolverFactory.getResolver();
    final DrillFuncHolder func = functionResolver.getBestMatch(functions, functionCall);

    final RelDataTypeFactory factory = opBinding.getTypeFactory();

    // least restrictive type (nullable ANY type)
    final RelDataType anyType = factory.createSqlType(SqlTypeName.ANY);
    final RelDataType nullableAnyType = factory.createTypeWithNullability(anyType, true);

    final MajorType returnType = func.getReturnType();
    if (NONE.equals(returnType)) {
      return nullableAnyType;
    }

    final MinorType minorType = returnType.getMinorType();
    final SqlTypeName sqlTypeName = DrillConstExecutor.DRILL_TO_CALCITE_TYPE_MAPPING.get(minorType);
    if (sqlTypeName == null) {
      return factory.createTypeWithNullability(nullableAnyType, true);
    }

    RelDataType relReturnType;
    switch (sqlTypeName) {
      case INTERVAL_DAY_TIME:
        relReturnType = factory.createSqlIntervalType(
            new SqlIntervalQualifier(
            TimeUnit.DAY,
            TimeUnit.MINUTE,
            SqlParserPos.ZERO));
        break;
      case INTERVAL_YEAR_MONTH:
        relReturnType = factory.createSqlIntervalType(
            new SqlIntervalQualifier(
            TimeUnit.YEAR,
            TimeUnit.MONTH,
            SqlParserPos.ZERO));
        break;
      default:
        relReturnType = factory.createSqlType(sqlTypeName);
    }

    switch(returnType.getMode()) {
      case OPTIONAL:
        relReturnType = factory.createTypeWithNullability(relReturnType, true);
        break;
      case REQUIRED:
        if(func.getNullHandling() == NullHandling.INTERNAL
            || (func.getNullHandling() == NullHandling.NULL_IF_NULL
                && opBinding.getOperandCount() > 0
                    && opBinding.getOperandType(0).isNullable())) {
          relReturnType = factory.createTypeWithNullability(relReturnType, true);
        }

        break;
      case REPEATED:
        break;
      default:
        throw new UnsupportedOperationException();
    }

    if(relReturnType.getSqlTypeName() == SqlTypeName.VARCHAR) {
      final boolean isNullable = relReturnType.isNullable();
      relReturnType = factory.createSqlType(SqlTypeName.VARCHAR, MAX_VARCHAR_LENGTH);

      if(isNullable) {
        relReturnType = factory.createTypeWithNullability(relReturnType, true);
      }
    }
    return relReturnType;
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    if(functions == null || functions.isEmpty() || opBinding.getOperator().getName().toUpperCase().equals("FALTTEN")) {
      return opBinding.getTypeFactory()
          .createTypeWithNullability(opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
    }

    final RelDataTypeFactory factory = opBinding.getTypeFactory();
    final String name = opBinding.getOperator().getName().toUpperCase();
    if(name.equals("CONCAT")) {
      final RelDataType type = factory.createSqlType(SqlTypeName.VARCHAR, MAX_VARCHAR_LENGTH);
      if(opBinding.getOperandType(0).isNullable()) {
        return factory.createTypeWithNullability(type, true);
      } else {
        return type;
      }
    } else if(name.equals("CONVERT_FROM")) {
      final RelDataType type = factory.createSqlType(SqlTypeName.ANY);
      if(opBinding.getOperandType(0).isNullable()) {
        return factory.createTypeWithNullability(type, true);
      } else {
        return type;
      }
    } else if(name.equals("DATE_PART")) {
      final String toType = opBinding.getOperandLiteralValue(0).toString().toUpperCase();
      assert toType.charAt(0) == '\'' && toType.charAt(toType.length() - 1) == '\'';

      final SqlTypeName sqlTypeName;
      switch(toType) {
        case "'SECOND'":
          sqlTypeName = SqlTypeName.DOUBLE;
          break;

        case "'MINUTE'":
        case "'HOUR'":
        case "'DAY'":
        case "'MONTH'":
        case "'YEAR'":
          sqlTypeName = SqlTypeName.BIGINT;
          break;
        default:
          throw new UnsupportedOperationException();
      }

      final RelDataType type = factory.createSqlType(sqlTypeName);
      if(opBinding.getOperandType(0).isNullable()) {
        return factory.createTypeWithNullability(type, true);
      } else {
        return type;
      }
    } else if(name.equals("EXTRACT")) {
        // Assert that the first argument to extract is a QuotedString
      assert opBinding.getOperandType(0) instanceof IntervalSqlType;
      final TimeUnit timeUnit = ((IntervalSqlType) opBinding.getOperandType(0)).getIntervalQualifier().getStartUnit();

      final SqlTypeName sqlTypeName;
      switch (timeUnit){
        case YEAR:
        case MONTH:
        case DAY:
        case HOUR:
        case MINUTE:
          sqlTypeName = SqlTypeName.BIGINT;
          break;
        case SECOND:
          sqlTypeName = SqlTypeName.DOUBLE;
          break;
          default:
            throw new UnsupportedOperationException("extract function supports the following time units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND");
        }

      final RelDataType type = factory.createSqlType(sqlTypeName);
      if(opBinding.getOperandType(0).isNullable()) {
        return factory.createTypeWithNullability(type, true);
      } else {
        return type;
      }
    }

    return getReturnType(opBinding, functions);
  }

  public final List<DrillFuncHolder> getFunctions() {
    return functions;
  }

  public static class RexCallFake extends RexCall {
    public RexCallFake(SqlOperator op, List<? extends RexNode> operands, final RelDataTypeFactory factory) {
      super(factory.createSqlType(SqlTypeName.ANY), op, operands);
    }
  }
}