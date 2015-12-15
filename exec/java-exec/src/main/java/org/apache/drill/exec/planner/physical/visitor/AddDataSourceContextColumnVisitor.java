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
package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.planner.physical.ScreenPrel;
import org.apache.drill.exec.planner.types.DrillRelDataTypeSystem;

import java.util.List;

public class AddDataSourceContextColumnVisitor extends BasePrelVisitor<Prel, Void, RuntimeException> {
  public static final String DATA_SOURCE_CONTEXT = "datasource";
  public static final RelDataType DATA_SOURCE_CONTEXT_TYPE =
      new BasicSqlType(DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM, SqlTypeName.ANY);

  private static AddDataSourceContextColumnVisitor INSTANCE = new AddDataSourceContextColumnVisitor();

  public static Prel addDataSourceContextColumnVisitor(Prel prel) {
    return prel.accept(INSTANCE, null);
  }

  @Override
  public Prel visitPrel(Prel prel, Void value) throws RuntimeException {
    final List<RelNode> children = Lists.newArrayList();
    for (Prel child : prel) {
      child = child.accept(this, null);
    }

    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitScreen(ScreenPrel prel, Void value) throws RuntimeException {
    final Prel child = ((Prel) prel.getInput()).accept(this, null);
    return new ScreenPrel(prel.getCluster(), prel.getTraitSet(), child);
  }

  @Override
  public Prel visitScan(ScanPrel prel, Void value) throws RuntimeException {
    final List<RelDataTypeField> fields = Lists.newArrayList(prel.getRowType().getFieldList());
    fields.add(new RelDataTypeFieldImpl(DATA_SOURCE_CONTEXT, fields.size(), DATA_SOURCE_CONTEXT_TYPE));
    return new ScanPrel(prel.getCluster(), prel.getTraitSet(), prel.getGroupScan(), new RelRecordType(fields));
  }

  @Override
  public Prel visitProject(ProjectPrel prel, Void value) throws RuntimeException {
    final Prel child = ((Prel) prel.getInput()).accept(this, null);

    final List<RelDataTypeField> projFields = Lists.newArrayList(prel.getRowType().getFieldList());
    projFields.add(new RelDataTypeFieldImpl(DATA_SOURCE_CONTEXT, projFields.size(), DATA_SOURCE_CONTEXT_TYPE));
    final List<RexNode> projList = Lists.newArrayList(prel.getChildExps());
    projList.add(new RexInputRef(prel.getRowType().getFieldCount(), DATA_SOURCE_CONTEXT_TYPE));
    prel = (ProjectPrel) prel.copy(prel.getTraitSet(), child, projList, new RelRecordType(projFields));
    return prel;
  }
}
