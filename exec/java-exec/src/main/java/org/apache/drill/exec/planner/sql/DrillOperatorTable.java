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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.ExplicitOperatorBinding;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlMonotonicBinaryOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Implementation of {@link SqlOperatorTable} that contains standard operators and functions provided through
 * {@link #inner SqlStdOperatorTable}, and Drill User Defined Functions.
 */
public class DrillOperatorTable extends SqlStdOperatorTable {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillOperatorTable.class);

  private static final SqlOperatorTable inner = SqlStdOperatorTable.instance();
  private List<SqlOperator> operators;
  private ArrayListMultimap<String, SqlOperator> opMap = ArrayListMultimap.create();

  public DrillOperatorTable(FunctionImplementationRegistry registry) {
    operators = Lists.newArrayList();
    operators.addAll(inner.getOperatorList());

    registry.register(this);
  }

  public void add(String name, SqlOperator op) {
    operators.add(op);
    opMap.put(name.toLowerCase(), op);
  }

  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category,
      SqlSyntax syntax, List<SqlOperator> operatorList) {

    final List<SqlOperator> calciteOperatorList = Lists.newArrayList();
    inner.lookupOperatorOverloads(opName, category, syntax, calciteOperatorList);

    for(final SqlOperator calciteOperator : calciteOperatorList) {
      final List<SqlOperator> drillSubstitute = getSubstituteFromDrillOptiq(calciteOperator);
      if(!drillSubstitute.isEmpty()) {
        operatorList.addAll(drillSubstitute);
      } else if(calciteOperator instanceof SqlAggFunction) {
        operatorList.add(new DrillCalciteSqlAggFunctionWrapper((SqlAggFunction) calciteOperator));
      } else if(calciteOperator instanceof SqlFunction) {
        operatorList.add(new DrillCalciteSqlFunctionWrapper((SqlFunction) calciteOperator));
      } else {
        operatorList.add(new DrillCalciteSqlOperatorWrapper(calciteOperator));
      }
    }

    // if no function is found, check in Drill UDFs
    if (operatorList.isEmpty() && syntax == SqlSyntax.FUNCTION && opName.isSimple()) {
      List<SqlOperator> drillOps = opMap.get(opName.getSimple().toLowerCase());
      if (drillOps != null && !drillOps.isEmpty()) {
        operatorList.addAll(drillOps);
      }
    }
  }

  @Override
  public List<SqlOperator> getOperatorList() {
    return operators;
  }

  // Get the list of SqlOperator's with the given name.
  public List<SqlOperator> getSqlOperator(String name) {
    return opMap.get(name.toLowerCase());
  }

  private List<SqlOperator> getSubstituteFromDrillOptiq(final SqlOperator calciteOperator) {
    final String functionName = calciteOperator.getName().toLowerCase();
    if(functionName.equals("extract") || functionName.equals("date_part")) {
      final List<SqlOperator> drillOps = opMap.get(functionName);
      return drillOps;
    } else {
      return Lists.newArrayList();
    }
  }
}
