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
package org.apache.drill.exec.planner.logical;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.eigenbase.rel.AggregateRel;
import org.eigenbase.rel.ProjectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.RelShuttleImpl;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.rel.TableFunctionRelBase;
import org.eigenbase.rel.ValuesRel;
import org.eigenbase.rel.FilterRel;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.SortRel;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexVisitor;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.SqlFunction;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlSingleValueAggFunction;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.NlsString;

/**
 * This class rewrites all the project expression that contain convert_to/ convert_from
 * to actual implementations.
 * Eg: convert_from(EXPR, 'JSON') is rewritten as convert_fromjson(EXPR)
 *
 * With the actual method name we can find out if the function has a complex
 * output type and we will fire/ ignore certain rules (merge project rule) based on this fact.
 */
public class RewriteProjectRel extends RelShuttleImpl {

  RelDataTypeFactory factory;
  DrillOperatorTable table;

  public RewriteProjectRel(RelDataTypeFactory factory, DrillOperatorTable table) {
    super();
    this.factory = factory;
    this.table = table;
  }

  @Override
  public RelNode visit(ProjectRel project) {
    checkOperators(project);

    List<RexNode> exprList = new ArrayList<>();
    boolean rewrite = false;

    for (RexNode rex : project.getChildExps()) {
      RexNode newExpr = rex;
      if (rex instanceof RexCall) {
        RexCall function = (RexCall) rex;
        String functionName = function.getOperator().getName();
        int nArgs = function.getOperands().size();

        // check if its a convert_from or convert_to function
        if (functionName.equalsIgnoreCase("convert_from") || functionName.equalsIgnoreCase("convert_to")) {
          assert nArgs == 2 && function.getOperands().get(1) instanceof RexLiteral;
          String literal = ((NlsString) (((RexLiteral) function.getOperands().get(1)).getValue())).getValue();
          RexBuilder builder = new RexBuilder(factory);

          // construct the new function name based on the input argument
          String newFunctionName = functionName + literal;

          // Look up the new function name in the drill operator table
          List<SqlOperator> operatorList = table.getSqlOperator(newFunctionName);
          assert operatorList.size() > 0;
          SqlFunction newFunction = null;

          // Find the SqlFunction with the correct args
          for (SqlOperator op : operatorList) {
            if (op.getOperandTypeChecker().getOperandCountRange().isValidCount(nArgs - 1)) {
              newFunction = (SqlFunction) op;
              break;
            }
          }
          assert newFunction != null;

          // create the new expression to be used in the rewritten project
          newExpr = builder.makeCall(newFunction, function.getOperands().subList(0, 1));
          rewrite = true;
        }
      }
      exprList.add(newExpr);
    }

    if (rewrite == true) {
      ProjectRel newProject = project.copy(project.getTraitSet(), project.getInput(0), exprList, project.getRowType());
      return visitChild(newProject, 0, project.getChild());
    }

    return visitChild(project, 0, project.getChild());
  }

  @Override
  public RelNode visit(AggregateRel aggregate) {
    checkOperators(aggregate);
    for(AggregateCall aggregateCall : aggregate.getAggCallList()) {
      if(aggregateCall.getAggregation() instanceof SqlSingleValueAggFunction) {
        throw new UnsupportedOperationException("Logical expression performed on non-scalar result of sub-query is not supported");
      }
    }

    return visitChild(aggregate, 0, aggregate.getChild());
  }

  @Override
  public RelNode visit(TableAccessRelBase scan) {
    checkOperators(scan);
    return scan;
  }

  @Override
  public RelNode visit(TableFunctionRelBase scan) {
    checkOperators(scan);
    return visitChildren(scan);
  }

  @Override
  public RelNode visit(ValuesRel values) {
    checkOperators(values);
    return values;
  }

  @Override
  public RelNode visit(FilterRel filter) {
    checkOperators(filter);
    return visitChild(filter, 0, filter.getChild());
  }

  @Override
  public RelNode visit(JoinRel join) {
    checkOperators(join);
    return visitChildren(join);
  }

  @Override
  public RelNode visit(SortRel sort) {
    checkOperators(sort);
    return visitChildren(sort);
  }

  @Override
  public RelNode visit(RelNode other) {
    checkOperators(other);
    return visitChildren(other);
  }

  private void checkOperators(RelNode relNode) {
    for (RexNode rex : relNode.getChildExps()) {
      if (rex instanceof RexCall) {
        RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitCall(RexCall call) {
            SqlOperator op = call.getOperator();
            List<RexNode> operands = call.getOperands();
            if(op.getName().equals("||") && (operands.get(0).getType().getSqlTypeName() == SqlTypeName.VARCHAR || operands.get(1).getType().getSqlTypeName() == SqlTypeName.VARCHAR)) {
              throw new UnsupportedOperationException(op + " is not supported");
            }
            return super.visitCall(call);
          }
        };
        visitor.visitCall((RexCall) rex);
      }
    }
  }
}
