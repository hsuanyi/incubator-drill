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
package org.apache.drill.exec.physical.impl.filter;

import javax.inject.Named;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.AbstractSkipRecordLogging;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.selection.SelectionVector2;

import java.io.IOException;
import java.util.Map;

public abstract class FilterTemplate2 implements Filterer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FilterTemplate2.class);

  private SelectionVector2 outgoingSelectionVector;
  private SelectionVector2 incomingSelectionVector;
  private SelectionVectorMode svMode;
  private TransferPair[] transfers;
  private boolean skipRecord;
  private AbstractSkipRecordLogging skipRecordLogging;

  @Override
  public void setup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing, TransferPair[] transfers, AbstractSkipRecordLogging skipRecordLogging) throws SchemaChangeException{
    this.transfers = transfers;
    this.outgoingSelectionVector = outgoing.getSelectionVector2();
    this.svMode = incoming.getSchema().getSelectionVectorMode();

    this.skipRecord = (skipRecordLogging != null);
    this.skipRecordLogging = skipRecordLogging;

    switch(svMode){
    case NONE:
      break;
    case TWO_BYTE:
      this.incomingSelectionVector = incoming.getSelectionVector2();
      break;
    default:
      // SV4 is handled in FilterTemplate4
      throw new UnsupportedOperationException();
    }
    doSetup(context, incoming, outgoing);
    this.skipRecord = (skipRecordLogging != null);
    this.skipRecordLogging = skipRecordLogging;
  }

  private void doTransfers(){
    for(TransferPair t : transfers){
      t.transfer();
    }
  }

  public void filterBatch(int recordCount){
    if (recordCount == 0) {
      return;
    }
    if (! outgoingSelectionVector.allocateNewSafe(recordCount)) {
      throw new OutOfMemoryException("Unable to allocate filter batch");
    }
    switch(svMode){
    case NONE:
      filterBatchNoSV(recordCount);
      break;
    case TWO_BYTE:
      filterBatchSV2(recordCount);
      break;
    default:
      throw new UnsupportedOperationException();
    }
    doTransfers();
  }

  private void filterBatchSV2(int recordCount){
    int svIndex = 0;
    final int count = recordCount;
    for(int i = 0; i < count; i++){
      char index = incomingSelectionVector.getIndex(i);
      final boolean keep;
      if(skipRecord) {
        keep = doEvalSkip(i, index, 0);
      } else {
        keep = doEval(index, 0);
      }

      if(keep){
        outgoingSelectionVector.setIndex(svIndex, index);
        svIndex++;
      }
    }
    outgoingSelectionVector.setRecordCount(svIndex);
  }

  private void filterBatchNoSV(int recordCount){
    int svIndex = 0;
    for(int i = 0; i < recordCount; i++){
      final boolean retain;
      if(skipRecord) {
        retain = doEvalSkip(i, i, 0);
      } else {
        retain = doEval(i, 0);
      }

      if(retain) {
        outgoingSelectionVector.setIndex(svIndex, (char)i);
        svIndex++;
      }
    }
    outgoingSelectionVector.setRecordCount(svIndex);
  }

  private boolean doEvalSkip(int inputline, int inIndex, int outIndex) {
    boolean keep;
    try {
      keep = doEval(inIndex, 0);
    } catch(Exception e) {
      keep = false;
      skipRecordLogging.incrementSkippedRecord();
      skipRecordLogging.setRowNumber(inputline);
      skipRecordLogging.append("Error_Type", e);
      try {
        skipRecordLogging.write();
      } catch (IOException ioe) {
        throw new RuntimeException();
      }
    }
    return keep;
  }

  public abstract void doSetup(@Named("context") FragmentContext context, @Named("incoming") RecordBatch incoming, @Named("outgoing") RecordBatch outgoing);
  public abstract boolean doEval(@Named("inIndex") int inIndex, @Named("outIndex") int outIndex);
}
