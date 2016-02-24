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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.scanner.persistence.AnnotatedClassDescriptor;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.planner.sql.DrillSqlAggOperator;
import org.apache.drill.exec.planner.sql.DrillSqlOperator;

import com.google.common.collect.ArrayListMultimap;

/**
 * Registry of Drill functions.
 */
public class DrillFunctionRegistry {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillFunctionRegistry.class);

  // key: function name (lowercase) value: list of functions with that name
  private final ArrayListMultimap<String, DrillFuncHolder> registeredFunctions = ArrayListMultimap.create();

  public DrillFunctionRegistry(ScanResult classpathScan) {
    FunctionConverter converter = new FunctionConverter();
    List<AnnotatedClassDescriptor> providerClasses = classpathScan.getAnnotatedClasses();

    // Hash map to prevent registering functions with exactly matching signatures
    // key: Function Name + Input's Major Type
    // value: Class name where function is implemented
    //
    final Map<String, String> functionSignatureMap = new HashMap<>();
    for (AnnotatedClassDescriptor func : providerClasses) {
      DrillFuncHolder holder = converter.getHolder(func);
      if (holder != null) {
        // register handle for each name the function can be referred to
        String[] names = holder.getRegisteredNames();

        // Create the string for input types
        String functionInput = "";
        for (DrillFuncHolder.ValueReference ref : holder.parameters) {
          functionInput += ref.getType().toString();
        }
        for (String name : names) {
          String functionName = name.toLowerCase();
          registeredFunctions.put(functionName, holder);
          String functionSignature = functionName + functionInput;
          String existingImplementation;
          if ((existingImplementation = functionSignatureMap.get(functionSignature)) != null) {
            throw new AssertionError(
                String.format(
                    "Conflicting functions with similar signature found. Func Name: %s, Class name: %s " +
                " Class name: %s", functionName, func.getClassName(), existingImplementation));
          } else if (holder.isAggregating() && !holder.isDeterministic() ) {
            logger.warn("Aggregate functions must be deterministic, did not register function {}", func.getClassName());
          } else {
            functionSignatureMap.put(functionSignature, func.getClassName());
          }
        }
      } else {
        logger.warn("Unable to initialize function for class {}", func.getClassName());
      }
    }
    if (logger.isTraceEnabled()) {
      StringBuilder allFunctions = new StringBuilder();
      for (DrillFuncHolder method: registeredFunctions.values()) {
        allFunctions.append(method.toString()).append("\n");
      }
      logger.trace("Registered functions: [\n{}]", allFunctions);
    }
  }

  public int size(){
    return registeredFunctions.size();
  }

  /** Returns functions with given name. Function name is case insensitive. */
  public List<DrillFuncHolder> getMethods(String name) {
    return this.registeredFunctions.get(name.toLowerCase());
  }

  public Collection<DrillFuncHolder> getAllMethods() {
    return Collections.unmodifiableCollection(registeredFunctions.values());
  }

  public void register(DrillOperatorTable operatorTable) {
    for (Entry<String, Collection<DrillFuncHolder>> function : registeredFunctions.asMap().entrySet()) {
      final ArrayListMultimap<Pair<Integer, Integer>, DrillFuncHolder> functions = ArrayListMultimap.create();
      final ArrayListMultimap<Integer, DrillFuncHolder> aggregateFunctions = ArrayListMultimap.create();
      final String name = function.getKey().toUpperCase();
      boolean isDeterministic = true;
      for (DrillFuncHolder func : function.getValue()) {
        final int paramCount = func.getParamCount();
        if(func.isAggregating()) {
          aggregateFunctions.put(paramCount, func);
        } else {
          final Pair<Integer, Integer> argNumerRange = getArgNumerRange(name, func);
          functions.put(argNumerRange, func);
        }

        // In order to prevent Drill from folding constant functions with types that cannot be materialized into literals,
        // the deterministic property of the DrillSqlOperator which has any least one function with NON_REDUCIBLE_TYPES will be set to false.
        //
        // However,  partition-pruning will be initiated "only" for the deterministic DrillSqlOperator.
        // Thus, an additional logic is added to PruneScanRule to help decide if partition-pruning can be taken.
        if(!func.isDeterministic()) {
          isDeterministic = false;
        }
      }
      for (Entry<Pair<Integer, Integer>, Collection<DrillFuncHolder>> entry : functions.asMap().entrySet()) {
        final DrillSqlOperator drillSqlOperator;
        final Pair<Integer, Integer> range = entry.getKey();
        final int max = range.getRight();
        final int min = range.getLeft();
        drillSqlOperator = new DrillSqlOperator(
            name,
            Lists.newArrayList(entry.getValue()),
            min,
            max,
            isDeterministic);
        operatorTable.add(name, drillSqlOperator);
      }
      for (Entry<Integer, Collection<DrillFuncHolder>> entry : aggregateFunctions.asMap().entrySet()) {
        operatorTable.add(name, new DrillSqlAggOperator(name, Lists.newArrayList(entry.getValue()), entry.getKey()));
      }
    }

    registerCalcitePlaceHolderFunction(operatorTable);
  }

  /**
   * These {@link DrillSqlOperator} merely act as a placeholder so that Calcite
   * allows convert_to(), convert_from(), flatten() functions in SQL.
   */
  private void registerCalcitePlaceHolderFunction(DrillOperatorTable operatorTable) {
    final String convert_to = "CONVERT_TO";
    final String convert_from = "CONVERT_FROM";
    final String flatten = "FLATTEN";

    operatorTable.add(convert_to,
        new DrillSqlOperator(convert_to,
            2,
            true));
    operatorTable.add(convert_from,
        new DrillSqlOperator(convert_from,
            2,
            true));
    operatorTable.add(flatten,
        new DrillSqlOperator(flatten,
            1,
            true));
  }

  private Pair<Integer, Integer> getArgNumerRange(final String name, final DrillFuncHolder func) {
    switch(name) {
      case "concat":
        return Pair.of(1, Integer.MAX_VALUE);

      // Drill does not have a FunctionTemplate for the lpad/rpad with two arguments.
      // It relies on DrillOptiq.java to add a third dummy argument to be acceptable
      // by the FunctionTemplate in StringFunctions.java
      case "lpad":
      case "rpad":
        return Pair.of(2, 3);

      // Similar to the reason above, DrillOptiq.java is used for rewritting
      case "ltrim":
      case "rtrim":
      case "btrim":
        return Pair.of(1, 2);

      // Similar to the reason above, DrillOptiq.java is used for rewritting
      case "length":
        return Pair.of(1, 2);

      default:
        return Pair.of(func.getParamCount(), func.getParamCount());
    }
  }
}
