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

import java.util.List;

import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.TranslatableTable;

import org.apache.drill.exec.dotdrill.View;
import org.apache.drill.exec.ops.ViewExpansionContext;
import org.apache.drill.exec.planner.sql.DrillCalciteSqlAggFunctionWrapper;
import org.apache.drill.exec.planner.sql.DrillCalciteSqlFunctionWrapper;
import org.apache.drill.exec.planner.sql.DrillCalciteSqlOperatorWrapper;

public class DrillViewTable implements TranslatableTable, DrillViewInfoProvider {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillViewTable.class);

  private final View view;
  private final String viewOwner;
  private final ViewExpansionContext viewExpansionContext;

  public DrillViewTable(View view, String viewOwner, ViewExpansionContext viewExpansionContext){
    this.view = view;
    this.viewOwner = viewOwner;
    this.viewExpansionContext = viewExpansionContext;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return view.getRowType(typeFactory);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
    ViewExpansionContext.ViewExpansionToken token = null;
    try {
      RelDataType rowType = relOptTable.getRowType();
      RelNode rel;

      if (viewExpansionContext.isImpersonationEnabled()) {
        token = viewExpansionContext.reserveViewExpansionToken(viewOwner);
        rel = context.expandView(rowType, view.getSql(), token.getSchemaTree(), view.getWorkspaceSchemaPath());
      } else {
        rel = context.expandView(rowType, view.getSql(), view.getWorkspaceSchemaPath());
      }

      // If the View's field list is not "*", create a cast.
      if (!view.isDynamic() && !view.hasStar()) {
        rel = RelOptUtil.createCastRel(rel, rowType, true);
      }

      return rel;//unwrap(rel, context.getCluster().getTypeFactory());
    } finally {
      if (token != null) {
        token.release();
      }
    }
  }


  @Override
  public TableType getJdbcTableType() {
    return TableType.VIEW;
  }

  @Override
  public String getViewSql() {
    return view.getSql();
  }

  private RelNode unwrap(final RelNode rel, final RelDataTypeFactory relDataTypeFactory) {
    return rel.accept(new RelShuttleImpl(){
      @Override
      public RelNode visit(LogicalAggregate aggregate) {
        List<AggregateCall> aggCalls = Lists.newArrayList();
        for(AggregateCall aggregateCall : aggregate.getAggCallList()) {
          if(aggregateCall.getAggregation() instanceof DrillCalciteSqlAggFunctionWrapper) {
            aggregateCall = AggregateCall.create(((DrillCalciteSqlAggFunctionWrapper) aggregateCall.getAggregation()).getOperator(),
                aggregateCall.isDistinct(),
                aggregateCall.getArgList(),
                aggregateCall.filterArg,
                aggregateCall.getType(),
                aggregateCall.getName());
          }
            aggCalls.add(aggregateCall);
          }


        aggregate = aggregate.copy(aggregate.getTraitSet(),
            aggregate.getInput(),
            aggregate.indicator,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggCalls);

        return visitChild(aggregate, 0, aggregate.getInput());
      }

      @Override
      public RelNode visit(LogicalFilter filter) {
        filter = filter.copy(filter.getTraitSet(), filter.getInput(), filter.getCondition().accept(
            new Vis(relDataTypeFactory)));
        return visitChild(filter, 0, filter.getInput());
      }

      @Override
      public RelNode visit(LogicalProject project) {
        final Vis vis = new Vis(relDataTypeFactory);
        final List<RexNode> newChildExps = Lists.newArrayList();
        for(RexNode rexNode : project.getChildExps()) {
          newChildExps.add(rexNode.accept(vis));
        }

        project = project.copy(project.getTraitSet(),
            project.getInput(),
            newChildExps,
            project.getRowType());
        return visitChild(project, 0, project.getInput());
      }

      @Override
      public RelNode visit(LogicalJoin join) {
        join = join.copy(join.getTraitSet(),
            join.getCondition().accept(new Vis(relDataTypeFactory)),
            join.getInputs().get(0),
            join.getInputs().get(1),
            join.getJoinType(),
            join.isSemiJoinDone());
        return visitChildren(join);
      }
    });
  }

  private static class Vis extends RexShuttle {
    private RelDataTypeFactory relDataTypeFactory;
    public Vis(RelDataTypeFactory relDataTypeFactory) {
      this.relDataTypeFactory = relDataTypeFactory;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      final RexCall rexCall = (RexCall) super.visitCall(call);
      if(rexCall.getOperator() instanceof DrillCalciteSqlFunctionWrapper) {
          RexBuilder rexBuilder = new RexBuilder(relDataTypeFactory);
        return rexBuilder.makeCall(
              ((DrillCalciteSqlFunctionWrapper) rexCall.getOperator()).getWrappedSqlFunction(),
              rexCall.getOperands());
        } else if(rexCall.getOperator() instanceof DrillCalciteSqlOperatorWrapper) {
          RexBuilder rexBuilder = new RexBuilder(relDataTypeFactory);
        return rexBuilder.makeCall(
            ((DrillCalciteSqlOperatorWrapper) rexCall.getOperator()).getWrappedSqlOperator(),
            rexCall.getOperands());
        } else {
        return rexCall;
      }
    }
  }
}
