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
import com.google.common.collect.Maps;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.drill.common.scanner.persistence.AnnotatedClassDescriptor;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionArgumentNumber;
import org.apache.drill.exec.planner.logical.DrillConstExecutor;
import org.apache.drill.exec.planner.sql.Checker;
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
      final Map<Integer, ArrayListMultimap<Integer, DrillFuncHolder>> functions = Maps.newHashMap();
      final Map<Integer, ArrayListMultimap<Integer, DrillFuncHolder>> aggregateFunctions = Maps.newHashMap();
      final String name = function.getKey().toUpperCase();
      boolean isDeterministic = false;
      for (DrillFuncHolder func : function.getValue()) {
        final int paramCount = func.getFunctionArgumentNumber() == FunctionArgumentNumber.VARIABLE ? -1 : func.getParamCount();
        if(func.isAggregating()) {
          if(!aggregateFunctions.containsKey(paramCount)) {
            final ArrayListMultimap<Integer, DrillFuncHolder> innerMap = ArrayListMultimap.create();
            aggregateFunctions.put(paramCount, innerMap);
          }

          final int optionalArgNum = func.getFunctionArgumentNumber().getNumOptionalField();
          aggregateFunctions.get(paramCount).put(optionalArgNum, func);
        } else {
          if(!functions.containsKey(paramCount)) {
            final ArrayListMultimap<Integer, DrillFuncHolder> innerMap = ArrayListMultimap.create();
            functions.put(paramCount, innerMap);
          }

          final int optionalArgNum = func.getFunctionArgumentNumber().getNumOptionalField();
          functions.get(paramCount).put(optionalArgNum, func);
        }
        // prevent Drill from folding constant functions with types that cannot be materialized
        // into literals
        if (DrillConstExecutor.NON_REDUCIBLE_TYPES.contains(func.getReturnType().getMinorType())) {
          isDeterministic = false;
        } else {
          isDeterministic = func.isDeterministic();
        }
      }

      for(Entry<Integer, ArrayListMultimap<Integer, DrillFuncHolder>> numArgToFnEntry : functions.entrySet()) {
        final int numArg = numArgToFnEntry.getKey();
        for (Entry<Integer, Collection<DrillFuncHolder>> numOptArgToFnEntry : numArgToFnEntry.getValue().asMap().entrySet()) {
          final int optArgNum = numOptArgToFnEntry.getKey();
          final SqlOperandTypeChecker sqlOperandTypeChecker;
          switch(optArgNum) {
            case -1:
              sqlOperandTypeChecker = new Checker();
              break;
            case 0:
              sqlOperandTypeChecker = new Checker(numArg);
              break;
            default:
              sqlOperandTypeChecker = new Checker(numArg - optArgNum, numArg);
              break;
          }

          operatorTable.add(name,
              new DrillSqlOperator(name, Lists.newArrayList(numOptArgToFnEntry.getValue()), sqlOperandTypeChecker, isDeterministic));
        }
      }

      for(Entry<Integer, ArrayListMultimap<Integer, DrillFuncHolder>> numArgToFnEntry : aggregateFunctions.entrySet()) {
        final int numArg = numArgToFnEntry.getKey();
        for (Entry<Integer, Collection<DrillFuncHolder>> numOptArgToFnEntry : numArgToFnEntry.getValue().asMap().entrySet()) {
          final int optArgNum = numOptArgToFnEntry.getKey();
          final SqlOperandTypeChecker sqlOperandTypeChecker;
          switch(optArgNum) {
            case -1:
              sqlOperandTypeChecker = new Checker();
              break;
            case 0:
              sqlOperandTypeChecker = new Checker(numArg);
              break;
            default:
              sqlOperandTypeChecker = new Checker(numArg - optArgNum, numArg);
              break;
          }

          operatorTable.add(name,
              new DrillSqlAggOperator(name, Lists.newArrayList(numOptArgToFnEntry.getValue()), sqlOperandTypeChecker));
        }
      }
    }
  }
}
