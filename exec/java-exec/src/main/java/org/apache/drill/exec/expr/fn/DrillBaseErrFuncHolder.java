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
package org.apache.drill.exec.expr.fn;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPrimitiveType;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class DrillBaseErrFuncHolder extends DrillSimpleFuncHolder {
  public static final String DRILL_ERROR_CODE = "DRILL_ERROR_CODE";
  public DrillBaseErrFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  protected JVar declareErrorVar(ClassGenerator<?> g, JBlock block) {
    JType intType = JPrimitiveType.parse(g.getModel(), "int");
    return block.decl(intType, DRILL_ERROR_CODE, JExpr.lit(0));
  }

  @Override
  protected ClassGenerator.HoldingContainer generateEvalBody(ClassGenerator<?> g, ClassGenerator.HoldingContainer[] inputVariables, String body, JVar[] workspaceJVars) {

    g.getEvalBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", registeredNames[0]));

    JBlock sub = new JBlock(true, true);
    JBlock topSub = sub;
    ClassGenerator.HoldingContainer out = null;
    TypeProtos.MajorType returnValueType = getReturnType();

    // Add error code
    JVar drillErr = declareErrorVar(g, sub);

    // add outside null handling if it is defined.
    if(nullHandling == FunctionTemplate.NullHandling.NULL_IF_NULL){
      JExpression e = null;
      for(ClassGenerator.HoldingContainer v : inputVariables){
        if(v.isOptional()){
          if(e == null){
            e = v.getIsSet();
          }else{
            e = e.mul(v.getIsSet());
          }
        }
      }

      if(e != null){
        // if at least one expression must be checked, set up the conditional.
        returnValueType = returnValue.type.toBuilder().setMode(TypeProtos.DataMode.OPTIONAL).build();
        out = g.declare(returnValueType);
        e = e.eq(JExpr.lit(0));
        JConditional jc = sub._if(e);
        jc._then().assign(out.getIsSet(), JExpr.lit(0));
        sub = jc._else();
      }
    }

    if(out == null) {
      out = g.declare(returnValueType);
    }
    // add the subblock after the out declaration.
    g.getEvalBlock().add(topSub);


    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValueType), returnValue.name, JExpr._new(g.getHolderType(returnValueType)));
    addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, false);

    addErrorHandlingBlock(g, sub, topSub, internalOutput, drillErr, out);

    g.getEvalBlock().directStatement(String.format("//---- end of eval portion of %s function. ----//", registeredNames[0]));

    return out;
  }

  protected abstract void addErrorHandlingBlock(ClassGenerator<?> g, JBlock sub, JBlock topSub, JVar internalOutput, JVar err, ClassGenerator.HoldingContainer out);
}
