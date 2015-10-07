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

import java.util.List;
import java.util.Set;

import javax.inject.Named;

import com.google.common.collect.Sets;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import com.google.common.collect.ImmutableList;

public abstract class ProjectorTemplate implements Projector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectorTemplate.class);

  private ImmutableList<TransferPair> transfers;
  private SelectionVector2 vector2;
  private SelectionVector4 vector4;
  private SelectionVectorMode svMode;
  private final Set<Integer> indicesToSkip = Sets.newHashSet();
  private boolean skipRecord = false;

  public ProjectorTemplate() throws SchemaChangeException {
  }

  @Override
  public final Set<Integer> getSkippedIndices() {
    return indicesToSkip;
  }

  @Override
  public final int projectRecords(int startIndex, final int recordCount, int firstOutputIndex) {
    switch (svMode) {
    case FOUR_BYTE:
      throw new UnsupportedOperationException();

    case TWO_BYTE:
      final int count = recordCount;
      for (int i = 0; i < count; i++, firstOutputIndex++) {
        if(skipRecord) {
          doEvalSkipIfBadData(vector2.getIndex(i), firstOutputIndex);
        } else {
          doEval(vector2.getIndex(i), firstOutputIndex);
        }
      }
      return recordCount;

    case NONE:
      final int countN = recordCount;
      int i;
      for (i = startIndex; i < startIndex + countN; i++, firstOutputIndex++) {
        if(skipRecord) {
          doEvalSkipIfBadData(i, firstOutputIndex);
        } else {
          doEval(i, firstOutputIndex);
        }
      }
      if (i < startIndex + recordCount || startIndex > 0) {
        for (TransferPair t : transfers) {
          t.splitAndTransfer(startIndex, i - startIndex);
        }
        return i - startIndex;
      }
      for (TransferPair t : transfers) {
          t.transfer();
      }
      return recordCount;

    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public final void setup(boolean skipRecord, FragmentContext context, RecordBatch incoming, RecordBatch outgoing, List<TransferPair> transfers)  throws SchemaChangeException{

    this.svMode = incoming.getSchema().getSelectionVectorMode();
    switch (svMode) {
    case FOUR_BYTE:
      this.vector4 = incoming.getSelectionVector4();
      break;
    case TWO_BYTE:
      this.vector2 = incoming.getSelectionVector2();
      break;
    }
    this.transfers = ImmutableList.copyOf(transfers);
    doSetup(context, incoming, outgoing);
    this.skipRecord = skipRecord;
  }

  private void doEvalSkipIfBadData(int inIndex, int outIndex) {
    try {
      doEval(inIndex, outIndex);
    } catch (Exception e) {
      indicesToSkip.add(outIndex);
      // TODO: Log bad data
    }
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract void doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
}
