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

import com.sun.codemodel.JAssignmentTarget;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JOp;
import com.sun.codemodel.JPrimitiveType;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DrillSimpleErrFuncHolder extends DrillSimpleFuncHolder {
  protected String drillFuncClass;
  public static final String DRILL_ERROR_CODE_ARRAY = "errorCodeList";
  public static final String DRILL_ERROR_CODE_TMP = "errorCodeTmp";
  public static final String DRILL_ERROR_CODE = "DRILL_ERROR_CODE";
  public static final String DRILL_ERROR_CODE_INDEX = "errorCodeIndex";

  public DrillSimpleErrFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
    drillFuncClass = checkNotNull(initializer.getClassName());
  }

  protected ClassGenerator.HoldingContainer generateEvalBody(ClassGenerator<?> g, ClassGenerator.HoldingContainer[] inputVariables, String body, JVar[] workspaceJVars) {
    g.getEvalBlock().directStatement(String.format("//---- start of eval portion of %s function. ----//", registeredNames[0]));

    JBlock sub = new JBlock(true, true);
    final JBlock topSub = sub;
    ClassGenerator.HoldingContainer out = null;
    TypeProtos.MajorType returnValueType = returnValue.type;

    // Add error code
    final JVar drillErr = sub.decl(JPrimitiveType.parse(g.getModel(), "int"),
        g.getNextVar(DRILL_ERROR_CODE), JExpr.lit(0));

    // add outside null handling if it is defined.
    if (nullHandling == FunctionTemplate.NullHandling.NULL_IF_NULL) {
      JExpression e = null;
      for (ClassGenerator.HoldingContainer v : inputVariables) {
        if (v.isOptional()) {
          JExpression isNullExpr;
          if (v.isReader()) {
            isNullExpr = JOp.cond(v.getHolder().invoke("isSet"), JExpr.lit(1), JExpr.lit(0));
          } else {
            isNullExpr = v.getIsSet();
          }
          if (e == null) {
            e = isNullExpr;
          } else {
            e = e.mul(isNullExpr);
          }
        }
      }

      if (e != null) {
        // if at least one expression must be checked, set up the conditional.
        returnValueType = returnValue.type.toBuilder().setMode(TypeProtos.DataMode.OPTIONAL).build();
        out = g.declare(returnValueType);
        e = e.eq(JExpr.lit(0));
        JConditional jc = sub._if(e);
        jc._then().assign(out.getIsSet(), JExpr.lit(0));
        sub = jc._else();
      }
    }

    if (out == null) {
      out = g.declare(returnValueType);
    }

    // add the subblock after the out declaration.
    g.getEvalBlock().add(topSub);


    JVar internalOutput = sub.decl(JMod.FINAL, g.getHolderType(returnValueType), returnValue.name, JExpr._new(g.getHolderType(returnValueType)));
    addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, false);
    if (sub != topSub) {
      sub.assign(internalOutput.ref("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode
    }
    sub.assign(out.getHolder(), internalOutput);

    addErrorHandlingBlock(g, sub, topSub, internalOutput, drillErr, out);
    g.getEvalBlock().directStatement(String.format("//---- end of eval portion of %s function. ----//", registeredNames[0]));

    return out;
  }

  protected void addErrorHandlingBlock(ClassGenerator<?> g, JBlock sub, JBlock topSub, JVar internalOutput, JVar err, ClassGenerator.HoldingContainer out) {
    JBlock noErrBlock = new JBlock();

    if (internalOutput != null) {
      if (sub != topSub) {
        noErrBlock.assign(internalOutput.ref("isSet"),JExpr.lit(1));// Assign null if NULL_IF_NULL mode}
      }
      noErrBlock.assign(out.getHolder(), internalOutput);
    }

    JConditional topCond = sub._if(err.ne(JExpr.lit(0)));
    topCond._then().add(getErrorBlock(g, err));
    topCond._else().add(noErrBlock);
  }

  private JBlock getErrorBlock(ClassGenerator<?> g, final JVar errorCode) {
    final JBlock errBlock = new JBlock();
    JFieldVar point = null;
    for(Map.Entry<String, JFieldVar> a : g.clazz.fields().entrySet()) {
      if(a.getKey().contains(DRILL_ERROR_CODE_ARRAY)) {
        point = a.getValue();
        break;
      }
    }
    assert point != null;

    JVar indexInErrorCodeArray = null;
    for(Object obj : g.getEvalBlock().getContents()) {
      if((obj instanceof JVar) && ((JVar) obj).name().contains(DRILL_ERROR_CODE_INDEX)) {
        indexInErrorCodeArray = (JVar) obj;
      }
    }
    assert indexInErrorCodeArray != null;

    final JClass intArrayType = JPrimitiveType.parse(g.getModel(), "int").array();
    final JVar intArray = errBlock.decl(intArrayType, "intArray");
    errBlock.assign(intArray, JExpr.cast(intArrayType, point.invoke("get").arg(indexInErrorCodeArray)));

    final JConditional topCond = errBlock._if(JExpr.lit(0).ne(intArray.component(JExpr.lit(0))));
    final JBlock conditionBlock = new JBlock();
    conditionBlock.assign(intArray.component(JExpr.lit(0)), errorCode);
    topCond._then().add(conditionBlock);

    return errBlock;
  }
}
