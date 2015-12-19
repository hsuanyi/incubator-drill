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
import com.sun.codemodel.JClass;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.annotations.FunctionErrors;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DrillSimpleErrFuncHolder extends DrillBaseErrFuncHolder {

  protected String drillFuncClass;

  public DrillSimpleErrFuncHolder(FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
    drillFuncClass = checkNotNull(initializer.getClassName());
  }

  private JBlock getErrorBlock(ClassGenerator<?> g, JVar internalOutput, JVar err) {
    JClass t = g.getModel().ref(drillFuncClass);
    JClass runTimeException = g.getModel().ref(DrillRuntimeException.class);
    JClass functionErrors = g.getModel().ref(FunctionErrors.class);

    // Block if we have an error code returned; will be added to the top level block
    JBlock errBlock = new JBlock();

    // Generic annotation object to get around JANINO
    JType obj = g.getModel().ref(Object.class);
    JType templateClass = g.getModel().ref(FunctionErrors.class);
    JVar annotation = errBlock.decl(templateClass, "annotations");
    JVar genericObject = errBlock.decl(obj,"annotationObj");

    // Get the annotation object from the class
    errBlock.assign(genericObject, JExpr.invoke(t.dotclass(), "getAnnotation").arg(functionErrors.dotclass()));

    // Type cast the annotation object to FunctionErrors
    errBlock.assign(annotation, JExpr.cast(templateClass, genericObject));

    JType strType = g.getModel()._ref(String.class).array();

    // From the annotations get the errors String array
    JVar errMsg1 = errBlock.decl(strType, "errorMsg", errBlock.invoke(annotation, "errors"));

    errBlock._throw(JExpr._new(runTimeException).arg(errMsg1.component(err)));

    return errBlock;
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
    topCond._then().add(getErrorBlock(g, internalOutput, err));
    topCond._else().add(noErrBlock);
  }
}
