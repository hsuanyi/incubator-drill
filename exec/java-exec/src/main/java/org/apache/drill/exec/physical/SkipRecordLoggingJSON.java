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
import org.apache.drill.exec.store.RecordReader;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public class SkipRecordLoggingJSON extends AbstractSkipRecordLogging {
  private FSDataOutputStream os;
  private JsonFactory jsonFactory;

  public SkipRecordLoggingJSON(FileSystem fs, String path, final long thresholdFailure) throws IOException {
    super(thresholdFailure);
    final Path p = new Path(path + "/a.json");

    os = fs.create(p);
    jsonFactory = new JsonFactory();
    jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
  }

  @Override
  public void write() throws IOException {
    final ObjectMapper mapper = new ObjectMapper(jsonFactory);
    final Map<String, String> map = Maps.newHashMap();
    for(Map.Entry<String, String> entry : this) {
      map.put(entry.getKey(), entry.getValue());
    }

    mapper.writerWithDefaultPrettyPrinter().writeValue(os, map);
    clear();
  }

  @Override
  public void flush() throws IOException {
    os.flush();
    os.close();
  }
}
