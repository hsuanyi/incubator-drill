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

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.math.BigDecimal;
import java.util.List;

public class DrillLimitZeroPushDownRule extends RelOptRule {
  public static final DrillLimitZeroPushDownRule INSTANCE =
      new DrillLimitZeroPushDownRule();
  private DrillLimitZeroPushDownRule() {
    super(operand(DrillLimitRel.class,
        operand(RelNode.class, any())));
  }

  public boolean matches(RelOptRuleCall call) {
    final DrillLimitRel drillLimitRel = call.rel(0);
    final RexNode fetch = drillLimitRel.getFetch();
    if(!(fetch instanceof RexLiteral)
        || !((RexLiteral) drillLimitRel.getFetch()).getValue().equals(BigDecimal.ZERO)) {
      return false;
    }

    final RelNode child = call.rel(1);
    switch (child.getInputs().size()) {
      case 1:
        return child instanceof DrillProjectRel
            || child instanceof DrillFilterRel
                || child instanceof DrillSortRel
                    || child instanceof DrillAggregateRel
                        || child instanceof DrillLimitRel;
      case 2:
        return child instanceof DrillUnionRel
            || child instanceof DrillJoinRel;
      default:
        return false;
    }
  }

  public void onMatch(RelOptRuleCall call) {
    final DrillLimitRel drillLimitRel = call.rel(0);
    final RelNode child = call.rel(1);
    final RelOptCluster cluster = drillLimitRel.getCluster();
    // Push Limit past RelNode child based on the number of inputs of child
    switch (child.getInputs().size()) {
      // If the RelNode child has only a single input,
      // swap them
      case 1: {
        final RelTraitSet relTraitSet = child.getTraitSet();
        final RelNode inputToChild = child.getInput(0);
        final List<RelNode> inputs = Lists.newArrayList();
        inputs.add(inputToChild);
        final RelNode pushedLimit = drillLimitRel.copy(relTraitSet, inputs);

        final List<RelNode> topNodeInput = Lists.newArrayList();
        topNodeInput.add(pushedLimit);
        final RelNode topNode = child.copy(relTraitSet, topNodeInput);
        call.transformTo(topNode);
        break;
      }
      case 2: {
        // Push Limit to both input sides of RelNode child
        final List<RelNode> leftLimitInput = Lists.newArrayList();
        final List<RelNode> rightLimitInput = Lists.newArrayList();
        leftLimitInput.add(child.getInput(0));
        rightLimitInput.add(child.getInput(1));

        final RelNode leftLimit = drillLimitRel.copy(
            child.getInput(0).getTraitSet(), leftLimitInput);
        final RelNode rightLimit = drillLimitRel.copy(
            child.getInput(1).getTraitSet(), rightLimitInput);

        final List<RelNode> topNodeInputs = Lists.newArrayList();
        topNodeInputs.add(leftLimit);
        topNodeInputs.add(rightLimit);
        RelNode topNode = child.copy(child.getTraitSet(), topNodeInputs);
        // If it is a Cartesian-Join, pushing limit past join and adding a dumb condition in the join
        // can eliminate the Cartesian-Join, without producing the wrong result
        if(topNode instanceof DrillJoinRel
            && ((DrillJoinRel) topNode).getCondition().isAlwaysTrue()) {
          final DrillJoinRel drillJoinRel = (DrillJoinRel) topNode;
          final RexBuilder builder = cluster.getRexBuilder();

          // Produce a dumb Join-Condition
          final List<RelDataTypeField> leftTypes = drillJoinRel.getInput(0).getRowType().getFieldList();
          final List<RelDataTypeField> rightTypes = drillJoinRel.getInput(1).getRowType().getFieldList();
          final int leftKeyOrdinal = 0;
          final int rightKeyOrdinal = 0;
          final int numLeftFields = drillJoinRel.getLeft().getRowType().getFieldCount();
          final List<RexNode> dumbJoinList = Lists.newArrayList();
          dumbJoinList.add(builder.makeCall(
              SqlStdOperatorTable.EQUALS,
                  builder.makeInputRef(leftTypes.get(leftKeyOrdinal).getType(), leftKeyOrdinal),
                      builder.makeInputRef(rightTypes.get(rightKeyOrdinal).getType(), rightKeyOrdinal + numLeftFields)));
          final RexNode dumbCondition = RexUtil.composeConjunction(builder, dumbJoinList, false);
          topNode = drillJoinRel.copy(drillJoinRel.getTraitSet(),
              dumbCondition,
                  drillJoinRel.getInput(0),
                      drillJoinRel.getInput(1),
                          drillJoinRel.getJoinType(),
                              drillJoinRel.isSemiJoinDone());
        }
        call.transformTo(topNode);
        break;
      }
      default:
        break;
    }
  }
}
