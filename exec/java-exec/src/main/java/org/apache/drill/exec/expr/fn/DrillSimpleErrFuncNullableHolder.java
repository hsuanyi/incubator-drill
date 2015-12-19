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
import com.sun.codemodel.JVar;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;

import java.util.List;

public class DrillSimpleErrFuncNullableHolder extends DrillBaseErrFuncHolder{
  public DrillSimpleErrFuncNullableHolder(DrillSimpleErrFuncHolder errFuncHolder) {
    super(errFuncHolder.attributes, errFuncHolder.initializer);
  }

 @Override
  public TypeProtos.MajorType getReturnType(List<LogicalExpression> args) {
    return TypeProtos.MajorType.newBuilder().setMinorType(returnValue.type.getMinorType()).setMode(TypeProtos.DataMode.OPTIONAL).build();
  }

  @Override
  public TypeProtos.MajorType getReturnType() {
    return returnValue.type.toBuilder().setMode(TypeProtos.DataMode.OPTIONAL).build();
  }

  @Override
  protected void addErrorHandlingBlock(ClassGenerator<?> g, JBlock sub, JBlock topSub, JVar internalOutput, JVar err, ClassGenerator.HoldingContainer out) {
    if (internalOutput != null) {
      JBlock noErrBlock = new JBlock();

      noErrBlock.assign(internalOutput.ref("isSet"),JExpr.lit(1));
      noErrBlock.assign(out.getHolder(), internalOutput);

      JConditional topCond = sub._if(err.ne(JExpr.lit(0)));
      topCond._then().assign(internalOutput.ref("isSet"), JExpr.lit(0));
      topCond._else().add(noErrBlock);
    }
  }
}
