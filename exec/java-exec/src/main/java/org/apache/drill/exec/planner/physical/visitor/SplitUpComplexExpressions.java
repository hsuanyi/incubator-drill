/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.planner.physical.visitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import net.hydromatic.optiq.tools.RelConversionException;

import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.PrelUtil.ProjectPushInfo;
import org.apache.drill.exec.planner.types.RelDataTypeDrillImpl;
import org.apache.drill.exec.planner.types.RelDataTypeHolder;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.eigenbase.rel.RelNode;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.reltype.RelDataTypeFieldImpl;
import org.eigenbase.reltype.RelRecordType;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.sql.type.SqlTypeName;
import java.util.List;

public class SplitUpComplexExpressions extends BasePrelVisitor<Prel, Object, RelConversionException> {
  RelDataTypeFactory factory;
  DrillOperatorTable table;
  FunctionImplementationRegistry funcReg;

  public SplitUpComplexExpressions(RelDataTypeFactory factory, DrillOperatorTable table, FunctionImplementationRegistry funcReg) {
    super();
    this.factory = factory;
    this.table = table;
    this.funcReg = funcReg;
  }

  @Override
  public Prel visitPrel(Prel prel, Object value) throws RelConversionException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }
    return (Prel) prel.copy(prel.getTraitSet(), children);
  }

  @Override
  public Prel visitProject(ProjectPrel project, Object unused) throws RelConversionException {
    // After transforming the complex expressions (each expression being transformed into a Project followed by a complex function),
    // Drill puts another Project (called top-level Project here)

    // Top-level Project's expression & DataType
    List<RexNode> exprList = Lists.newArrayList();
    List<RelDataTypeField> origRelDataTypes = Lists.newArrayList();

    ProjectPushInfo columnInfo = PrelUtil.getColumns(project.getInput(0).getRowType(), project.getProjects());
    if (columnInfo == null) {
      return project;
    }

    // The RelNode below the top-level Project
    RelNode originalInput = ((Prel) project.getChild()).accept(this, null);

    // Get the number of columns into the considered project
    int numIncomingColumn = originalInput.getRowType().getFieldList().size();
    RexVisitorComplexExprSplitter exprSplitter = new RexVisitorComplexExprSplitter(factory, funcReg, numIncomingColumn);

    // Populate exprList to project the right columns for the Top-level Project
    List<RexNode> childExps = project.getChildExps();
    for(int i = 0; i < childExps.size(); ++i) {
      origRelDataTypes.add(project.getRowType().getFieldList().get(i));
      exprList.add(childExps.get(i).accept(exprSplitter));
    }

    List<RexNode> complexExprs = exprSplitter.getComplexExprs();
    if (complexExprs.size() == 0) {
      return (Prel) project.copy(project.getTraitSet(), originalInput, exprList, new RelRecordType(origRelDataTypes));
    } else {
      // Insert (project, complex function) stack
      // between top-level Project and originalInput
      originalInput = insertingProjectComplexFunction(originalInput, project, complexExprs, numIncomingColumn);
      return new ProjectPrel(project.getCluster(), project.getTraitSet(), originalInput,
          ImmutableList.copyOf(exprList), new RelRecordType(origRelDataTypes));
    }
  }

  private RelNode insertingProjectComplexFunction(RelNode originalInput, ProjectPrel project, List<RexNode> complexExprs, int lastRexInput) {
    ProjectPrel childProject;
    List<RexNode> allExprs = Lists.newArrayList();
    List<RelDataTypeField> relDataTypes = Lists.newArrayList();

    int i = 0;
    int exprIndex = 0;
    List<String> fieldNames =  originalInput.getRowType().getFieldNames();
    while(i < lastRexInput) {
      RexBuilder builder = new RexBuilder(factory);
      allExprs.add(builder.makeInputRef(new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory), i));

      if(fieldNames.get(i).contains(StarColumnHelper.STAR_COLUMN)) {
        relDataTypes.add(new RelDataTypeFieldImpl(fieldNames.get(i), allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));
      } else {
        relDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + exprIndex, allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));
        ++exprIndex;
      }

      ++i;
    }

    // If the projection expressions contained complex outputs,
    // split them into their own individual projects
    while (complexExprs.size() > 0) {
      if (i >= lastRexInput) {
        // Remove the complex function which was already applied in the last iteration
        allExprs.remove(allExprs.size() - 1);

        // Add a new expression to project the column,
        // which contains the output of the complex function
        RexBuilder builder = new RexBuilder(factory);
        allExprs.add(builder.makeInputRef( new RelDataTypeDrillImpl(new RelDataTypeHolder(), factory), allExprs.size()));
      }

      // Get the complex expression
      RexNode newComplexExpr = complexExprs.remove(0);
      allExprs.add(newComplexExpr);
      relDataTypes.add(new RelDataTypeFieldImpl("EXPR$" + exprIndex, allExprs.size(), factory.createSqlType(SqlTypeName.ANY)));

      // Put the complex expression in its own project and
      // Wrap the original rel with this newly generated project
      childProject = new ProjectPrel(project.getCluster(), project.getTraitSet(), originalInput, ImmutableList.copyOf(allExprs), new RelRecordType(relDataTypes));
      originalInput = childProject;

      ++i;
      ++exprIndex;
    }

    return originalInput;
  }
}
