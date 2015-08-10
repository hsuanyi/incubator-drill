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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.Lists;

import java.util.List;

public class DrillWindowRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new DrillWindowRule();

  private DrillWindowRule() {
    super(RelOptHelper.some(Window.class, Convention.NONE, RelOptHelper.any(RelNode.class)), "DrillWindowRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Window window = call.rel(0);
    final RelNode input = call.rel(1);
    final RelTraitSet traits = window.getTraitSet().plus(DrillRel.DRILL_LOGICAL);
    final RelNode convertedInput = convert(input, traits);

    // If there is no need to reorder the window group,
    // just transform it to DrillWindowRel
    if(!needWindowReordering(window.groups)) {
      call.transformTo(
          new DrillWindowRel(
          window.getCluster(),
          traits,
          convertedInput,
          window.constants,
          window.getRowType(),
          window.groups));
    } else {
      // Ensure that the windows with no Partition-By should be processed
      final int colNum = convertedInput.getRowType().getFieldCount();

      // windowGroups contains the same groups as window.groups
      // The only different is the order where the windows without Partition-By
      // are always in front of those with Partition-By
      final List<Window.Group> windowGroups = Lists.newArrayList();

      // Since the order of windows is changed,
      // RowType would change also
      // windowFields is used to keep track of the new order of RowType
      final List<RelDataTypeField> windowFields = Lists.newArrayList(convertedInput.getRowType().getFieldList());
      int indexNewRowType = colNum;
      int indexNewGroup = 0;
      for(int i = 0, j = colNum; i < window.groups.size(); ++i) {
        final Window.Group windowGroup = window.groups.get(i);
        if (windowGroup.keys.isEmpty()) {
          windowGroups.add(indexNewGroup++, windowGroup);
          for (int k = 0; k < windowGroup.aggCalls.size(); ++k) {
            windowFields.add(indexNewRowType++,
                window.getRowType().getFieldList().get(j++));
          }
        } else {
          windowGroups.add(windowGroup);
          for (int k = 0; k < windowGroup.aggCalls.size(); ++k) {
            windowFields.add(window.getRowType().getFieldList().get(j++));
          }
        }
      }

      final DrillWindowRel windowRel = new DrillWindowRel(
          window.getCluster(),
          traits,
          convertedInput,
          window.constants,
          new RelRecordType(windowFields),
          windowGroups);

      // It is necessary to put a DrillProjectRel on top of windowRel
      // to ensure the order would not be changed at the final output
      final List<RexNode> projList = Lists.newArrayList();
      for (int i = 0; i < colNum; ++i) {
        projList.add(new RexInputRef(i, windowFields.get(i).getType()));
      }

      int indexOPart = colNum;
      int indexWPart = indexNewRowType;
      for (Window.Group windowGroup : window.groups) {
        if (windowGroup.keys.isEmpty()) {
          for (int i = 0; i < windowGroup.aggCalls.size(); ++i) {
            projList.add(new RexInputRef(indexOPart,
                windowFields.get(indexOPart).getType()));
            ++indexOPart;
          }
        } else {
          for (int i = 0; i < windowGroup.aggCalls.size(); ++i) {
            projList.add(new RexInputRef(indexWPart,
                windowFields.get(indexWPart).getType()));
            ++indexWPart;
          }
        }
      }

      call.transformTo(DrillProjectRel.create(
          windowRel.getCluster(),
          windowRel.getTraitSet(),
          windowRel,
          projList,
          window.getRowType()
          ));
    }
  }

  /**
   * In the given window groups, if a window with Partition-By appears
   * in front of a window which does not have Partition-By, this method
   * will return true. Otherwise, return false
   */
  private boolean needWindowReordering(List<Window.Group> groups) {
    for(int i = 1; i < groups.size(); ++i) {
      if(groups.get(i).keys.isEmpty()) {
        for(int j = 0; j < i; ++j) {
          if(!groups.get(j).keys.isEmpty()) {
            return true;
          }
        }
      }
    }

    return false;
  }
}
