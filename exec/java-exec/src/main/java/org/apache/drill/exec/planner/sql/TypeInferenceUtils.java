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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.MajorTypeInLogicalExpression;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.resolver.FunctionResolver;
import org.apache.drill.exec.resolver.FunctionResolverFactory;

import java.util.List;

public class TypeInferenceUtils {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeInferenceUtils.class);

  public static final TypeProtos.MajorType UNKNOWN_TYPE = TypeProtos.MajorType.getDefaultInstance();
  public static final int MAX_VARCHAR_LENGTH = 65535;

  private static ImmutableMap<TypeProtos.MinorType, SqlTypeName> DRILL_TO_CALCITE_TYPE_MAPPING =
      ImmutableMap.<TypeProtos.MinorType, SqlTypeName> builder()
          .put(TypeProtos.MinorType.INT, SqlTypeName.INTEGER)
          .put(TypeProtos.MinorType.BIGINT, SqlTypeName.BIGINT)
          .put(TypeProtos.MinorType.FLOAT4, SqlTypeName.FLOAT)
          .put(TypeProtos.MinorType.FLOAT8, SqlTypeName.DOUBLE)
          .put(TypeProtos.MinorType.VARCHAR, SqlTypeName.VARCHAR)
          .put(TypeProtos.MinorType.BIT, SqlTypeName.BOOLEAN)
          .put(TypeProtos.MinorType.DATE, SqlTypeName.DATE)
          .put(TypeProtos.MinorType.DECIMAL9, SqlTypeName.DECIMAL)
          .put(TypeProtos.MinorType.DECIMAL18, SqlTypeName.DECIMAL)
          .put(TypeProtos.MinorType.DECIMAL28SPARSE, SqlTypeName.DECIMAL)
          .put(TypeProtos.MinorType.DECIMAL38SPARSE, SqlTypeName.DECIMAL)
          .put(TypeProtos.MinorType.TIME, SqlTypeName.TIME)
          .put(TypeProtos.MinorType.TIMESTAMP, SqlTypeName.TIMESTAMP)
          .put(TypeProtos.MinorType.VARBINARY, SqlTypeName.VARBINARY)
          .put(TypeProtos.MinorType.INTERVALYEAR, SqlTypeName.INTERVAL_YEAR_MONTH)
          .put(TypeProtos.MinorType.INTERVALDAY, SqlTypeName.INTERVAL_DAY_TIME)
          .put(TypeProtos.MinorType.MAP, SqlTypeName.MAP)
          .put(TypeProtos.MinorType.LIST, SqlTypeName.ARRAY)
          .put(TypeProtos.MinorType.LATE, SqlTypeName.ANY)
          // These are defined in the Drill type system but have been turned off for now
          // .put(TypeProtos.MinorType.TINYINT, SqlTypeName.TINYINT)
          // .put(TypeProtos.MinorType.SMALLINT, SqlTypeName.SMALLINT)
          // Calcite types currently not supported by Drill, nor defined in the Drill type list:
          //      - CHAR, SYMBOL, MULTISET, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST
          .build();

  private static ImmutableMap<SqlTypeName, TypeProtos.MinorType> CALCITE_TO_DRILL_MAPPING =
      ImmutableMap.<SqlTypeName, TypeProtos.MinorType> builder()
          .put(SqlTypeName.INTEGER, TypeProtos.MinorType.INT)
          .put(SqlTypeName.BIGINT, TypeProtos.MinorType.BIGINT)
          .put(SqlTypeName.FLOAT, TypeProtos.MinorType.FLOAT4)
          .put(SqlTypeName.DOUBLE, TypeProtos.MinorType.FLOAT8)
          .put(SqlTypeName.VARCHAR, TypeProtos.MinorType.VARCHAR)
          .put(SqlTypeName.BOOLEAN, TypeProtos.MinorType.BIT)
          .put(SqlTypeName.DATE, TypeProtos.MinorType.DATE)
          .put(SqlTypeName.TIME, TypeProtos.MinorType.TIME)
          .put(SqlTypeName.TIMESTAMP, TypeProtos.MinorType.TIMESTAMP)
          .put(SqlTypeName.VARBINARY, TypeProtos.MinorType.VARBINARY)
          .put(SqlTypeName.INTERVAL_YEAR_MONTH, TypeProtos.MinorType.INTERVALYEAR)
          .put(SqlTypeName.INTERVAL_DAY_TIME, TypeProtos.MinorType.INTERVALDAY)
          .put(SqlTypeName.CHAR, TypeProtos.MinorType.VARCHAR)

          // The following types are not added due to a variety of reasons:
          // (1) Disabling decimal type
          //.put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL9)
          //.put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL18)
          //.put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL28SPARSE)
          //.put(SqlTypeName.DECIMAL, TypeProtos.MinorType.DECIMAL38SPARSE)

          // (2) These 2 types are defined in the Drill type system but have been turned off for now
          // .put(SqlTypeName.TINYINT, TypeProtos.MinorType.TINYINT)
          // .put(SqlTypeName.SMALLINT, TypeProtos.MinorType.SMALLINT)

          // (3) Calcite types currently not supported by Drill, nor defined in the Drill type list:
          //      - SYMBOL, MULTISET, DISTINCT, STRUCTURED, ROW, OTHER, CURSOR, COLUMN_LIST
          //.put(SqlTypeName.MAP, TypeProtos.MinorType.MAP)
          //.put(SqlTypeName.ARRAY, TypeProtos.MinorType.LIST)
          .build();

  /**
   * Given a Drill's TypeProtos.MinorType, return a Calcite's corresponding SqlTypeName
   */
  public static SqlTypeName getCalciteTypeFromDrillType(final TypeProtos.MinorType type) {
    return DRILL_TO_CALCITE_TYPE_MAPPING.get(type);
  }

  /**
   * Given a Calcite's RelDataType, return a Drill's corresponding TypeProtos.MinorType
   */
  public static TypeProtos.MinorType getDrillTypeFromCalciteType(final RelDataType relDataType) {
    final SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    TypeProtos.MinorType minorType = CALCITE_TO_DRILL_MAPPING.get(sqlTypeName);
    if(minorType == null) {
      minorType = TypeProtos.MinorType.LATE;
    }
    return minorType;
  }

  public static SqlReturnTypeInference getDrillSqlReturnTypeInference(
      final String name,
      final ArrayListMultimap<String, SqlOperator> opMap) {
    final List<DrillFuncHolder> functions = Lists.newArrayList();
    for(SqlOperator sqlOperator : opMap.get(name.toLowerCase())) {
      if(sqlOperator instanceof DrillSqlOperator
              && ((DrillSqlOperator) sqlOperator).getFunctions() != null) {
        functions.addAll(((DrillSqlOperator) sqlOperator).getFunctions());
      }
    }

    return getDrillSqlReturnTypeInference(name, functions);
  }

  public static SqlReturnTypeInference getDrillSqlReturnTypeInference(
      final String name,
      final List<DrillFuncHolder> functions) {
    switch(name.toUpperCase()) {
      case "DATE_PART":
        return DrillDatePartSqlReturnTypeInference.INSTANCE;

      case "SUM":
        return new DrillSUMSqlReturnTypeInference(functions);

      case "CONCAT":
        return DrillConcatSqlReturnTypeInference.INSTANCE;

      case "CONVERT_TO":
        return DrillConvertToSqlReturnTypeInference.INSTANCE;

      case "EXTRACT":
        return DrillExtractSqlReturnTypeInference.INSTANCE;

      case "CAST":
        return DrillCastSqlReturnTypeInference.INSTANCE;

      case "FLATTEN":
      case "KVGEN":
      case "CONVERT_FROM":
        return DrillDynamicOutputSqlReturnTypeInference.INSTANCE;

      default:
        return new DrillDefaultSqlReturnTypeInference(functions);
    }
  }

  private static class DrillDefaultSqlReturnTypeInference implements SqlReturnTypeInference {
    private final List<DrillFuncHolder> functions;

    public DrillDefaultSqlReturnTypeInference(List<DrillFuncHolder> functions) {
      this.functions = functions;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      if (functions.isEmpty()) {
        return factory.createTypeWithNullability(
            opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
      }


      boolean allBooleanOutput = true;
      for(DrillFuncHolder function : functions) {
        if(function.getReturnType().getMinorType() != TypeProtos.MinorType.BIT) {
          allBooleanOutput = false;
          break;
        }
      }
      if(allBooleanOutput) {
        return factory
            .createSqlType(SqlTypeName.BOOLEAN);
      }

      // The following logic is just a safe play:
      // Even if any of the input arguments has ANY type,
      // it "might" still be possible to determine the return type based on other non-ANY types
      for (RelDataType type : opBinding.collectOperandTypes()) {
        if (type.getSqlTypeName() == SqlTypeName.ANY || type.getSqlTypeName() == SqlTypeName.DECIMAL) {
          return factory
              .createTypeWithNullability(factory.createSqlType(SqlTypeName.ANY), true);
        }
      }

      final DrillFuncHolder func = resolveDrillFuncHolder(opBinding, functions);

      // If the return type is VarChar,
      // set the precision as the maximum
      RelDataType returnType = getReturnType(opBinding, func);
      if (returnType.getSqlTypeName() == SqlTypeName.VARCHAR) {
        final boolean isNullable = returnType.isNullable();
        returnType = factory.createSqlType(SqlTypeName.VARCHAR, MAX_VARCHAR_LENGTH);

        if (isNullable) {
          returnType = factory.createTypeWithNullability(returnType, true);
        }
      }

      return returnType;
    }

    private static RelDataType getReturnType(final SqlOperatorBinding opBinding, final DrillFuncHolder func) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();

      // least restrictive type (nullable ANY type)
      final RelDataType anyType = factory.createSqlType(SqlTypeName.ANY);
      final RelDataType nullableAnyType = factory.createTypeWithNullability(anyType, true);

      final TypeProtos.MajorType returnType = func.getReturnType();
      if (UNKNOWN_TYPE.equals(returnType)) {
        return nullableAnyType;
      }

      final TypeProtos.MinorType minorType = returnType.getMinorType();
      final SqlTypeName sqlTypeName = getCalciteTypeFromDrillType(minorType);
      if (sqlTypeName == null) {
        return factory.createTypeWithNullability(nullableAnyType, true);
      }

      final RelDataType relReturnType;
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
          break;
      }

      switch (returnType.getMode()) {
        case OPTIONAL:
          return factory.createTypeWithNullability(relReturnType, true);
        case REQUIRED:
          switch (func.getNullHandling()) {
            case INTERNAL:
              return relReturnType;
            case NULL_IF_NULL:
              boolean isNull = false;
              for (int i = 0; i < opBinding.getOperandCount(); ++i) {
                if (opBinding.getOperandType(i).isNullable()) {
                  isNull = true;
                  break;
                }
              }

              if (isNull) {
                return factory.createTypeWithNullability(relReturnType, true);
              } else {
                return relReturnType;
              }
            default:
              throw new UnsupportedOperationException();
          }
        case REPEATED:
          return relReturnType;
        default:
          throw new UnsupportedOperationException();
      }
    }
  }

  private static class DrillDynamicOutputSqlReturnTypeInference implements SqlReturnTypeInference {
    private static DrillDynamicOutputSqlReturnTypeInference INSTANCE = new DrillDynamicOutputSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      return opBinding
          .getTypeFactory()
          .createTypeWithNullability(opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY), true);
    }
  }

  private static class DrillSUMSqlReturnTypeInference implements SqlReturnTypeInference {
    private final List<DrillFuncHolder> functions;
    public DrillSUMSqlReturnTypeInference(List<DrillFuncHolder> functions) {
      this.functions = functions;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final DrillFuncHolder drillFuncHolder = resolveDrillFuncHolder(opBinding, functions);
      final TypeProtos.MinorType minorType = drillFuncHolder
          .getReturnType()
          .getMinorType();
      final RelDataType relDataType = opBinding
          .getTypeFactory()
          .createSqlType(getCalciteTypeFromDrillType(minorType));

      // If there is group-by and the imput type is Non-nullable,
      // the output is Non-nullable;
      // Otherwise, the output is nullable.
      if(opBinding.getGroupCount() > 0 && !opBinding.getOperandType(0).isNullable()) {
        return relDataType;
      } else {
        return factory.createTypeWithNullability(relDataType, true);
      }
    }
  }

  private static class DrillConcatSqlReturnTypeInference implements SqlReturnTypeInference {
    private static DrillConcatSqlReturnTypeInference INSTANCE = new DrillConcatSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final RelDataType type = factory.createSqlType(
          SqlTypeName.VARCHAR,
          TypeInferenceUtils.MAX_VARCHAR_LENGTH);

      boolean isNullable = true;
      for(RelDataType relDataType : opBinding.collectOperandTypes()) {
        if(!relDataType.isNullable()) {
          isNullable = false;
          break;
        }
      }

      if(isNullable) {
        return factory.createTypeWithNullability(type, true);
      } else {
        return type;
      }
    }
  }

  private static class DrillConvertToSqlReturnTypeInference implements SqlReturnTypeInference {
    private static DrillConvertToSqlReturnTypeInference INSTANCE = new DrillConvertToSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final RelDataType type = factory.createSqlType(
          SqlTypeName.VARBINARY);
      if(opBinding.getOperandType(0).isNullable()) {
        return factory.createTypeWithNullability(type, true);
      } else {
        return type;
      }
    }
  }

  private static class DrillExtractSqlReturnTypeInference implements SqlReturnTypeInference {
    private static DrillExtractSqlReturnTypeInference INSTANCE = new DrillExtractSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final TimeUnit timeUnit = opBinding.getOperandType(0).getIntervalQualifier().getStartUnit();
      final boolean isNullable = opBinding.getOperandType(1).isNullable();

      final SqlTypeName sqlTypeName = getSqlTypeNameForTimeUnit(timeUnit.name());
      final RelDataType type;
      if(isNullable) {
        type = factory.createTypeWithNullability(
            factory.createSqlType(sqlTypeName), true);
      } else {
        type = factory.createSqlType(sqlTypeName);
      }
      return type;
    }

    private static SqlTypeName getSqlTypeNameForTimeUnit(String timeUnit) {
      switch (timeUnit.toUpperCase()){
        case "YEAR":
        case "MONTH":
        case "DAY":
        case "HOUR":
        case "MINUTE":
          return SqlTypeName.BIGINT;
        case "SECOND":
          return SqlTypeName.DOUBLE;
        default:
          throw UserException
              .functionError()
              .message("extract function supports the following time units: YEAR, MONTH, DAY, HOUR, MINUTE, SECOND")
              .build(logger);
      }
    }
  }

  private static class DrillDatePartSqlReturnTypeInference implements SqlReturnTypeInference {
    private static DrillDatePartSqlReturnTypeInference INSTANCE = new DrillDatePartSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();

      final SqlNode firstOperand = ((SqlCallBinding) opBinding).operand(0);
      if(!(firstOperand instanceof SqlCharStringLiteral)) {
        final RelDataType type;
        if(opBinding.getOperandType(1).isNullable()) {
          type = factory.createTypeWithNullability(
              factory.createSqlType(SqlTypeName.ANY), true);
        } else {
          type = factory.createSqlType(SqlTypeName.ANY);
        }
        return type;
      }

      final String part = ((SqlCharStringLiteral) firstOperand)
          .getNlsString()
          .getValue()
          .toUpperCase();

      final SqlTypeName sqlTypeName = DrillExtractSqlReturnTypeInference.getSqlTypeNameForTimeUnit(part);

      final boolean isNullable = opBinding.getOperandType(1).isNullable();
      final RelDataType type;
      if(isNullable) {
        type = factory.createTypeWithNullability(
            factory.createSqlType(sqlTypeName), true);
      } else {
        type = factory.createSqlType(sqlTypeName);
      }
      return type;
    }
  }

  private static class DrillCastSqlReturnTypeInference implements SqlReturnTypeInference {
    private static DrillCastSqlReturnTypeInference INSTANCE = new DrillCastSqlReturnTypeInference();

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final boolean isNullable = opBinding
          .getOperandType(0)
          .isNullable();

      return opBinding
          .getTypeFactory()
          .createTypeWithNullability(
              opBinding.getOperandType(1),
              isNullable);
    }
  }

  private static DrillFuncHolder resolveDrillFuncHolder(final SqlOperatorBinding opBinding, final List<DrillFuncHolder> functions) {
    final List<LogicalExpression> args = Lists.newArrayList();
    for (final RelDataType type : opBinding.collectOperandTypes()) {
      final TypeProtos.MinorType minorType = getDrillTypeFromCalciteType(type);
      final TypeProtos.MajorType majorType;
      if (type.isNullable()) {
        majorType =  Types.optional(minorType);
      } else {
        majorType = Types.required(minorType);
      }

      args.add(new MajorTypeInLogicalExpression(majorType));
    }
    final FunctionCall functionCall = new FunctionCall(opBinding.getOperator().getName(), args, ExpressionPosition.UNKNOWN);
    final FunctionResolver functionResolver = FunctionResolverFactory.getResolver();
    final DrillFuncHolder func = functionResolver.getBestMatch(functions, functionCall);

    // Throw an exception
    // if no DrillFuncHolder matched for the given list of operand types
    if(func == null) {
      String operandTypes = "";
      for(int i = 0; i < opBinding.getOperandCount(); ++i) {
        operandTypes += opBinding.getOperandType(i).getSqlTypeName();
        if(i < opBinding.getOperandCount() - 1) {
          operandTypes += ",";
        }
      }

      throw UserException
          .functionError()
          .message(String.format("%s does not support operand types (%s)",
              opBinding.getOperator().getName(),
              operandTypes))
          .build(logger);
    }
    return func;
  }

  /**
   * This class is not intended to be initiated
   */
  private TypeInferenceUtils() {
  }
}