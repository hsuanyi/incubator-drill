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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.Maps;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.work.foreman.UnsupportedDataTypeException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public class SkipRecordLoggingJSON implements AutoCloseable {
  private FileSystem fs;
  private FSDataOutputStream os = null;
  private JsonFactory jsonFactory;
  private final String filePath;

  private int numSkippedRecord = 0;
  private final long thresholdFailure;
  private String sourceLocation = null;
  private int offset = 1;

  public static SkipRecordLoggingJSON create(FragmentContext context) throws ExecutionSetupException, IOException{
    final StoragePlugin storagePlugin = context.getDrillbitContext().getStorage().getPlugin("dfs");
    if (!(storagePlugin instanceof FileSystemPlugin) || storagePlugin.supportsWrite()) {
      throw new UnsupportedDataTypeException("");
    }

    final FileSystemPlugin plugin = (FileSystemPlugin) storagePlugin;
    final DrillFileSystem fs = new DrillFileSystem(plugin.getFsConf());

    final String qid = "" + context.getHandle().getQueryId().getPart1() + context.getHandle().getQueryId().getPart2();
    final String minorFrag = context.getHandle().getMajorFragmentId() + "-" + context.getHandle().getMinorFragmentId();

    return new SkipRecordLoggingJSON(
        fs,
        "/Users/hyichu/Desktop/skip",
        minorFrag,
        qid,
        context.getDrillbitContext().getEndpoint().getAddress(),
        context.getOptions().getOption(ExecConstants.SKIP_INVALID_RECORD_THRESHOLD));
  }

  private SkipRecordLoggingJSON(FileSystem fs, String path, String minorFragment, String qid, String nodeId, long thresholdFailure) throws IOException {
    this.fs = fs;
    this.filePath = path + "/" + qid + "/" + nodeId + "/" + minorFragment + "/skipprecords.json";
    jsonFactory = new JsonFactory();
    jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    this.thresholdFailure = thresholdFailure;;
  }

  public final void incrementSkippedRecord(int increment) {
    numSkippedRecord += increment;
    if(numSkippedRecord > thresholdFailure) {
      throw new IllegalStateException("The number of offending records exceeds the threshold");
    }
  }

  public void setSourceLocation(String sourceLocation) {
    if(!this.sourceLocation.equals(sourceLocation)) {
      this.sourceLocation = sourceLocation;
      this.offset = 0;
    }
  }

  public void incrementOffset() {
    ++this.offset;
  }

  public final void write(String errorMessage) {
    try {
      if(os == null) {
        final Path p = new Path(filePath);
        os = fs.create(p);
      }

      final ObjectMapper mapper = new ObjectMapper(jsonFactory);
      final Map<String, Object> map = Maps.newHashMap();
      map.put("File_Name", this.sourceLocation);
      map.put("Row_Number", this.offset);
      map.put("Error_Type", errorMessage);
      mapper.writerWithDefaultPrettyPrinter().writeValue(os, map);
    } catch (IOException ioe) {
      throw new IllegalStateException();
    }
  }

  @Override
  public void close() {
    if(os != null) {
      try {
        os.flush();
        os.close();
      } catch (Exception e) {
        throw new UnsupportedOperationException();
      }
    }
  }
}
