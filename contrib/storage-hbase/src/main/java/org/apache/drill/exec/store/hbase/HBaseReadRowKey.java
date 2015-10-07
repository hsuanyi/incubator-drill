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
package org.apache.drill.exec.store.hbase;

import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;

import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.ExchangePrel;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.ScreenPrel;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

import java.util.List;

public abstract class HBaseReadRowKey extends StoragePluginOptimizerRule {
  private HBaseReadRowKey(RelOptRuleOperand operand, String nodeName) {
    super(operand, "HBaseRowKeyScan:" + nodeName);
  }

  public static final String ROW_KEY_SKIP = DrillHBaseConstants.ROW_KEY + "_skip";
  public static final RelDataType ROW_KEY_TYPE = new BasicSqlType(DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM, SqlTypeName.ANY);

  public static final StoragePluginOptimizerRule HBASE_READ_ROWKEY_SCAN
      = new HBaseReadRowKeyScan(
          RelOptHelper.some(Prel.class,
              RelOptHelper.any(ScanPrel.class)));

  public static final StoragePluginOptimizerRule HBASE_READ_ROWKEY_PROJECT
      = new HBaseReadRowKeyProject(
          RelOptHelper.some(Prel.class,
              RelOptHelper.some(ProjectPrel.class,
                  RelOptHelper.any(Prel.class))));


  public static final StoragePluginOptimizerRule HBASE_READ_ROWKEY_FILTER
      = new HBaseReadRowKeyFilter(
          RelOptHelper.some(Prel.class,
              RelOptHelper.any(FilterPrel.class)));

  public static class HBaseReadRowKeyScan extends HBaseReadRowKey {
    public HBaseReadRowKeyScan(RelOptRuleOperand operand) {
      super(operand, "SCAN");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final Prel prel = call.rel(0);
      final ScanPrel scanPrel = call.rel(1);

      if((prel instanceof ScreenPrel) || (prel instanceof ExchangePrel)) {
        return false;
      }

      final List<String> fieldNames = Lists.newArrayList();
      fieldNames.add(DrillHBaseConstants.ROW_KEY);
      fieldNames.add(StarColumnHelper.STAR_COLUMN);
      return !containsFieldNames(scanPrel.getRowType(), fieldNames);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Prel prel = call.rel(0);
      final ScanPrel scanPrel = (ScanPrel) call.rel(1);

      final List<RelDataTypeField> newFields = Lists.newArrayList(scanPrel.getRowType().getFieldList());
      newFields.add(new RelDataTypeFieldImpl(DrillHBaseConstants.ROW_KEY, scanPrel.getRowType().getFieldCount(), ROW_KEY_TYPE));
      final List<SchemaPath> newSchemaPathList = Lists.newArrayList(((HBaseGroupScan) scanPrel.getGroupScan()).getColumns());
      newSchemaPathList.add(SchemaPath.getSimplePath(DrillHBaseConstants.ROW_KEY));

      final ScanPrel newScanPrel = ScanPrel.create(
          scanPrel,
          scanPrel.getTraitSet(),
          scanPrel.getGroupScan().clone(newSchemaPathList),
          new RelRecordType(newFields));

      if(prel instanceof ProjectPrel) {
        final ProjectPrel origProject = (ProjectPrel) prel;

        final List<RelDataTypeField> projFields = Lists.newArrayList(origProject.getRowType().getFieldList());
        projFields.add(new RelDataTypeFieldImpl(ROW_KEY_SKIP, origProject.getRowType().getFieldCount(), ROW_KEY_TYPE));
        final List<RexNode> projList = Lists.newArrayList(origProject.getChildExps());
        projList.add(new RexInputRef(origProject.getRowType().getFieldCount(), ROW_KEY_TYPE));

        call.transformTo(origProject.copy(origProject.getTraitSet(), newScanPrel, projList, new RelRecordType(projFields)));
      } else {
        final List<RexNode> projList = Lists.newArrayList();
        for(int i = 0; i < newFields.size(); ++i) {
          projList.add(new RexInputRef(i, newFields.get(i).getType()));
        }

        final List<RelDataTypeField> projFields = Lists.newArrayList(scanPrel.getRowType().getFieldList());
        projFields.add(new RelDataTypeFieldImpl(ROW_KEY_SKIP, scanPrel.getRowType().getFieldCount(), ROW_KEY_TYPE));

        final ProjectPrel topProject = new ProjectPrel(
            scanPrel.getCluster(),
            scanPrel.getTraitSet(),
            newScanPrel,
            projList,
            new RelRecordType(projFields));

        final List<RelNode> inputs = Lists.newArrayList();
        inputs.add(topProject);
        call.transformTo(prel.copy(prel.getTraitSet(), inputs));
      }
    }
  }

  public static class HBaseReadRowKeyProject extends HBaseReadRowKey {
    public HBaseReadRowKeyProject(RelOptRuleOperand operand) {
      super(operand, "PROJECT");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final Prel prel = call.rel(0);
      final ProjectPrel projectPrel = call.rel(1);
      if((prel instanceof ScreenPrel) || (prel instanceof ExchangePrel)) {
        return false;
      }

      final List<String> fieldNames = Lists.newArrayList();
      fieldNames.add(ROW_KEY_SKIP);
      return !containsFieldNames(projectPrel.getRowType(), fieldNames);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Prel prel = call.rel(0);
      ProjectPrel projectPrel = call.rel(1);
      Prel bottom = call.rel(2);

      int index_ROW_KEY_SKIP = -1;
      for(int i = 0; i < bottom.getRowType().getFieldList().size(); ++i) {
        final RelDataTypeField field = bottom.getRowType().getFieldList().get(i);
        if(field.getName().equals(ROW_KEY_SKIP)) {
          index_ROW_KEY_SKIP = i;
          break;
        }
      }
      assert index_ROW_KEY_SKIP >= 0;

      final List<RexNode> projList = Lists.newArrayList(projectPrel.getChildExps());
      projList.add(new RexInputRef(index_ROW_KEY_SKIP, ROW_KEY_TYPE));
      final List<RelDataTypeField> projFieldList = Lists.newArrayList(projectPrel.getRowType().getFieldList());
      projFieldList.add(new RelDataTypeFieldImpl(ROW_KEY_SKIP, projFieldList.size(), ROW_KEY_TYPE));

      projectPrel = (ProjectPrel) projectPrel.copy(
          projectPrel.getTraitSet(),
              bottom,
                  projList,
                      new RelRecordType(projFieldList));

      final List<RelNode> inputs = Lists.newArrayList();
      inputs.add(projectPrel);
      call.transformTo(prel.copy(prel.getTraitSet(), inputs));
    }
  }

  public static class HBaseReadRowKeyFilter extends HBaseReadRowKey {
    public HBaseReadRowKeyFilter(RelOptRuleOperand operand) {
      super(operand, "Filter");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      final FilterPrel filterPrel = call.rel(1);
      final List<String> fieldNames = Lists.newArrayList();
      fieldNames.add(ROW_KEY_SKIP);

      if(!containsFieldNames(filterPrel.getRowType(), fieldNames)) {
        return true;
      } else {
        return true;

      }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
    }
  }

  public static boolean containsFieldNames(final RelDataType relDataType, final List<String> fieldNames) {
    for(RelDataTypeField relDataTypeField : relDataType.getFieldList()) {
      for(String fieldName : fieldNames) {
        if(relDataTypeField.getName().equals(fieldName)) {
          return true;
        }
      }
    }
    return false;
  }
}
