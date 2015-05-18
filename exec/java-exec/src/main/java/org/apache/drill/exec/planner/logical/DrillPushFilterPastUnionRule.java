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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;

import java.util.List;

import com.google.common.collect.Lists;

public class DrillPushFilterPastUnionRule extends RelOptRule {
  public final static RelOptRule INSTANCE = new DrillPushFilterPastUnionRule();
  protected DrillPushFilterPastUnionRule() {
    super(
        operand(
            DrillFilterRel.class,
                operand(DrillUnionRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule

  @Override
  public void onMatch(RelOptRuleCall call) {
    DrillFilterRel filterRel = (DrillFilterRel) call.rel(0);
    DrillUnionRel unionRel = (DrillUnionRel) call.rel(1);
    RexCall condition = (RexCall) filterRel.getCondition();

    // For each input side of the Union,
    // insert a filter above the original input RelNode
    List<RelNode> newUnionInputs = Lists.newArrayList();
    for(RelNode origInput : unionRel.getInputs()) {
      newUnionInputs.add(filterRel.copy(origInput.getTraitSet(), origInput, condition));
    }

    DrillUnionRel newUnion = (DrillUnionRel) unionRel.copy(filterRel.getTraitSet(), newUnionInputs);
    call.transformTo(newUnion);
  }
}
