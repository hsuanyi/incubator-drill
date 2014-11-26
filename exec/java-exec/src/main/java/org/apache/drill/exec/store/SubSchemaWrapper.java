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
package org.apache.drill.exec.store;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import net.hydromatic.optiq.Function;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.Table;

import org.apache.drill.exec.planner.logical.CreateTableEntry;

import com.google.common.collect.ImmutableList;

public class SubSchemaWrapper extends AbstractSchema {

  private final AbstractSchema innerSchema;

  public SubSchemaWrapper(AbstractSchema innerSchema, ExecutorService executor) {
    super(ImmutableList.<String>of(), innerSchema.getFullSchemaName(), executor);
    this.innerSchema = innerSchema;
  }

  @Override
  public boolean showInInformationSchema() {
    return false;
  }

  @Override
  public AbstractSchema getDefaultSchema() {
    return innerSchema.getDefaultSchema();
  }

  @Override
  public CreateTableEntry createNewTable(String tableName) {
    return innerSchema.createNewTable(tableName);
  }

  @Override
  public Collection<Function> getFunctions(String name) {
    return innerSchema.getFunctions(name);
  }

  @Override
  public Set<String> getFunctionNames() {
    return innerSchema.getFunctionNames();
  }

  @Override
  public Schema getSubSchema(String name) {
    return innerSchema.getSubSchema(name);
  }

  @Override
  public Set<String> getSubSchemaNames() {
    return innerSchema.getSubSchemaNames();
  }

  @Override
  public boolean isMutable() {
    return innerSchema.isMutable();
  }

  @Override
  protected Table safeGetTable(String name) {
    return innerSchema.getTable(name);
  }

  @Override
  protected Set<String> safeGetTableNames() {
    return innerSchema.getTableNames();
  }

  @Override
  public String getTypeName() {
    return innerSchema.getTypeName();
  }

}
