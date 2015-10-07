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
package org.apache.drill.exec.physical.impl.project;

import org.apache.drill.exec.compile.TemplateClassDefinition;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import javax.inject.Named;
import java.util.Set;

public abstract class RecordSkipperTemplate implements RecordSkipper {
  private SelectionVector2 sv;
  @Override
  public void setup(FragmentContext context, RecordBatch outRecord, SelectionVector2 sv) {
    this.sv = sv;
    doSetup(context, outRecord, outRecord);
  }

  @Override
  public int skipRecords(final int recordCount) {
    final int count = recordCount;
      for (int i = 0; i < count; i++) {
              doEval(sv.getIndex(i), i);
      }


    return recordCount;
  }

  public static final TemplateClassDefinition<RecordSkipper> TEMPLATE_DEFINITION = new TemplateClassDefinition<RecordSkipper>(RecordSkipper.class, RecordSkipperTemplate.class);
  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
}
