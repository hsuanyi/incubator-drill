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
package org.apache.drill.exec.physical;

import com.google.common.collect.Maps;

import org.apache.drill.exec.store.RecordReader;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractSkipRecordLogging implements Iterable<Map.Entry<String, String>> {
  private RecordReader.ReaderContext readerContext;
  private Map<String, String> recordInfo = Maps.newHashMap();
  private int numSkippedRecord = 0;
  private final long thresholdFailure;

  public AbstractSkipRecordLogging(final long thresholdFailure) {
    this.thresholdFailure = thresholdFailure;
  }

  public final void incrementSkippedRecord() {
    if(numSkippedRecord == thresholdFailure) {
      throw new IllegalStateException("The number of offending records exceeds the threshold");
    }

    ++numSkippedRecord;
  }

  public final int getSkippedRecord() {
    return numSkippedRecord;
  }

  public final void setRowNumber(final int rowNumber) {
    recordInfo.put("Row_Number", readerContext.getRowIdentifier(rowNumber));
  }

  public final void append(String key, Exception e) {
    recordInfo.put(key, e.toString());
  }

  public final void setReaderContext(RecordReader.ReaderContext readerContext) {
    this.readerContext = readerContext;
  }

  public abstract void write() throws IOException;
  public abstract void flush() throws IOException;

  public final void clear() {
    recordInfo.clear();
  }

  public Iterator<Map.Entry<String, String>> iterator() {
    return recordInfo.entrySet().iterator();
  }
}