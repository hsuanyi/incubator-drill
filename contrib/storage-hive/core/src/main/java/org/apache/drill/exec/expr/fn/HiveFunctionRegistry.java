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

import java.util.HashSet;
import java.util.Set;
import java.util.Collection;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.fn.impl.hive.ObjectInspectorHelper;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.planner.sql.HiveUDFOperator;
import org.apache.drill.exec.planner.sql.TypeInferenceUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

public class HiveFunctionRegistry implements PluggableFunctionRegistry{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveFunctionRegistry.class);

  private ArrayListMultimap<String, Class<? extends GenericUDF>> methodsGenericUDF = ArrayListMultimap.create();
  private ArrayListMultimap<String, Class<? extends UDF>> methodsUDF = ArrayListMultimap.create();
  private HashSet<Class<?>> nonDeterministicUDFs = new HashSet<>();

  /**
   * Scan the classpath for implementation of GenericUDF/UDF interfaces,
   * extracts function annotation and store the
   * (function name) --> (implementation class) mappings.
   * @param config
   */
  public HiveFunctionRegistry(DrillConfig config) {
    // TODO: see if we can avoid this. We can't change the constructor right now.
    ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
    Set<Class<? extends GenericUDF>> genericUDFClasses = classpathScan.getImplementations(GenericUDF.class);
    for (Class<? extends GenericUDF> clazz : genericUDFClasses) {
      register(clazz, methodsGenericUDF);
    }

    Set<Class<? extends UDF>> udfClasses = classpathScan.getImplementations(UDF.class);
    for (Class<? extends UDF> clazz : udfClasses) {
      register(clazz, methodsUDF);
    }
  }

  @Override
  public void register(DrillOperatorTable operatorTable) {
    for (String name : Sets.union(methodsGenericUDF.asMap().keySet(), methodsUDF.asMap().keySet())) {
      Collection<Class<? extends GenericUDF>> gUDFs = methodsGenericUDF.asMap().get(name);
      Collection<Class<? extends UDF>> uDFs = methodsUDF.asMap().get(name);
      operatorTable.add(name, new HiveUDFOperator(name.toUpperCase(), new HiveSqlReturnTypeInference(gUDFs, uDFs)));
    }
  }

  private <C,I> void register(Class<? extends I> clazz, ArrayListMultimap<String,Class<? extends I>> methods) {
    Description desc = clazz.getAnnotation(Description.class);
    String[] names;
    if (desc != null) {
      names = desc.name().split(",");
      for (int i=0; i<names.length; i++) {
        names[i] = names[i].trim();
      }
    }else{
      names = new String[]{clazz.getName().replace('.', '_')};
    }

    UDFType type = clazz.getAnnotation(UDFType.class);
    if (type != null && type.deterministic()) {
      nonDeterministicUDFs.add(clazz);
    }


    for(int i=0; i<names.length;i++) {
      methods.put(names[i].toLowerCase(), clazz);
    }
  }

  /**
   * Find the UDF class for given function name and check if it accepts the given input argument
   * types. If a match is found, create a holder and return
   * @param call
   * @return
   */
  @Override
  public HiveFuncHolder getFunction(FunctionCall call) {
    HiveFuncHolder h;

    h = resolveFunction(call, false);
    if (h != null) {
      return h;
    }

    return resolveFunction(call, true);
  }

  /**
   * Helper method which resolves the given function call to a Hive UDF. It takes an argument
   * <i>convertVarCharToVar16Char</i> which tells to implicitly cast input arguments of type VARCHAR to VAR16CHAR
   * and search Hive UDF registry using implicitly casted argument types.
   *
   * TODO: This is a rudimentary function resolver. Need to include more implicit casting such as DECIMAL28 to
   * DECIMAL38 as Hive UDFs can accept only DECIMAL38 type.
   */
  private HiveFuncHolder resolveFunction(FunctionCall call, boolean convertVarCharToVar16Char) {
    HiveFuncHolder holder;
    MajorType[] argTypes = new MajorType[call.args.size()];
    ObjectInspector[] argOIs = new ObjectInspector[call.args.size()];
    for (int i=0; i<call.args.size(); i++) {
      try {
        argTypes[i] = call.args.get(i).getMajorType();
        if (convertVarCharToVar16Char && argTypes[i].getMinorType() == MinorType.VARCHAR) {
          argTypes[i] = Types.withMode(MinorType.VAR16CHAR, argTypes[i].getMode());
        }
        argOIs[i] = ObjectInspectorHelper.getDrillObjectInspector(argTypes[i].getMode(), argTypes[i].getMinorType());
      } catch(Exception e) {
        // Hive throws errors if there are unsupported types. Consider there is no hive UDF supporting the
        // given argument types
        logger.trace("Failed to find a hive function for given FunctionCall: '{}'", call.toString(), e);
        return null;
      }
    }

    String funcName = call.getName().toLowerCase();

    // search in GenericUDF list
    for (Class<? extends GenericUDF> clazz: methodsGenericUDF.get(funcName)) {
      holder = matchAndCreateGenericUDFHolder(clazz, argTypes, argOIs);
      if (holder != null) {
        return holder;
      }
    }

    // search in UDF list
    for (Class<? extends UDF> clazz : methodsUDF.get(funcName)) {
      holder = matchAndCreateUDFHolder(call.getName(), clazz, argTypes, argOIs);
      if (holder != null) {
        return holder;
      }
    }

    return null;
  }

  private HiveFuncHolder matchAndCreateGenericUDFHolder(Class<? extends GenericUDF> udfClazz,
                                              MajorType[] argTypes,
                                              ObjectInspector[] argOIs) {
    // probe UDF to find if the arg types and acceptable
    // if acceptable create a holder object
    try {
      GenericUDF udfInstance = udfClazz.newInstance();
      ObjectInspector returnOI = udfInstance.initialize(argOIs);
      return new HiveFuncHolder(
        udfClazz,
        argTypes,
        returnOI,
        Types.optional(ObjectInspectorHelper.getDrillType(returnOI)),
        nonDeterministicUDFs.contains(udfClazz));
    } catch (IllegalAccessException | InstantiationException e) {
      logger.debug("Failed to instantiate class", e);
    } catch (Exception e) { /*ignore this*/ }

    return null;
  }

  private HiveFuncHolder matchAndCreateUDFHolder(String udfName,
                                                 Class<? extends UDF> udfClazz,
                                                 MajorType[] argTypes,
                                                 ObjectInspector[] argOIs) {
    try {
      GenericUDF udfInstance = new GenericUDFBridge(udfName, false/* is operator */, udfClazz.getName());
      ObjectInspector returnOI = udfInstance.initialize(argOIs);

      return new HiveFuncHolder(
        udfName,
        udfClazz,
        argTypes,
        returnOI,
        Types.optional(ObjectInspectorHelper.getDrillType(returnOI)),
        nonDeterministicUDFs.contains(udfClazz));
    } catch (Exception e) { /*ignore this*/ }

    return null;
  }

  public class HiveSqlReturnTypeInference implements SqlReturnTypeInference {
    private final Collection<Class<? extends GenericUDF>> gUDFs;
    private final Collection<Class<? extends UDF>> uDFs;
    private HiveSqlReturnTypeInference(Collection<Class<? extends GenericUDF>> gUDFs, Collection<Class<? extends UDF>> uDFs) {
      this.gUDFs = gUDFs;
      this.uDFs = uDFs;
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
      final RelDataTypeFactory factory = opBinding.getTypeFactory();
      final RelDataType anyType = factory.createTypeWithNullability(
          factory.createSqlType(SqlTypeName.ANY),
          true);

      // The following logic is just a safe play:
      // Even if any of the input arguments has ANY type,
      // it "might" still be possible to determine the return type based on other non-ANY types
      for (RelDataType type : opBinding.collectOperandTypes()) {
        if (TypeInferenceUtils.getDrillTypeFromCalciteType(type) == TypeProtos.MinorType.LATE) {
          return anyType;
        }
      }

      final RelDataType returnDataType = getReturnType(opBinding, false);
      if(!returnDataType.equals(anyType)) {
        return returnDataType;
      } else {
        return getReturnType(opBinding, true);
      }
    }

    private RelDataType getReturnType(SqlOperatorBinding opBinding, boolean convertVarCharToVar16Char) {
      final int argLength = opBinding.getOperandCount();
      TypeProtos.MajorType[] argTypes = new TypeProtos.MajorType[argLength];
      ObjectInspector[] argOIs = new ObjectInspector[argLength];
      for (int i = 0; i < argLength; ++i) {
        try {
          final RelDataType type = opBinding.getOperandType(i);
          final TypeProtos.MinorType minorType = TypeInferenceUtils.getDrillTypeFromCalciteType(type);

          TypeProtos.MajorType.Builder majorTypeBuilder = TypeProtos.MajorType.newBuilder().setMinorType(minorType);
          if(type.isNullable()) {
            majorTypeBuilder = majorTypeBuilder.setMode(TypeProtos.DataMode.OPTIONAL);
          } else {
            majorTypeBuilder = majorTypeBuilder.setMode(TypeProtos.DataMode.REQUIRED);
          }

          argTypes[i] = majorTypeBuilder.build();
          if (convertVarCharToVar16Char && argTypes[i].getMinorType() == TypeProtos.MinorType.VARCHAR) {
            argTypes[i] = Types.withMode(TypeProtos.MinorType.VAR16CHAR, argTypes[i].getMode());
          }
          argOIs[i] = ObjectInspectorHelper.getDrillObjectInspector(argTypes[i].getMode(), argTypes[i].getMinorType());
        } catch(Exception e) {
          throw getFunctionNotfoundException(opBinding);
        }
      }

      // search in GenericUDF list
      if(gUDFs != null) {
        for (Class<? extends GenericUDF> clazz: gUDFs) {
          final HiveFuncHolder holder = matchAndCreateGenericUDFHolder(clazz,
              argTypes,
              argOIs);
          if (holder != null) {
            return TypeInferenceUtils.createCalciteTypeWithNullability(
                opBinding.getTypeFactory(),
                TypeInferenceUtils.getCalciteTypeFromDrillType(holder.getReturnType().getMinorType()),
                holder.getReturnType().getMode() != TypeProtos.DataMode.REQUIRED);
          }
        }
      }

      // search in UDF list
      if(uDFs != null) {
        for (Class<? extends UDF> clazz : uDFs) {
          final HiveFuncHolder holder = matchAndCreateUDFHolder(opBinding.getOperator().getName().toLowerCase(),
              clazz,
              argTypes,
              argOIs);
          if (holder != null) {
            return TypeInferenceUtils.createCalciteTypeWithNullability(
                opBinding.getTypeFactory(),
                TypeInferenceUtils.getCalciteTypeFromDrillType(holder.getReturnType().getMinorType()),
                holder.getReturnType().getMode() != TypeProtos.DataMode.REQUIRED);
          }
        }
      }


      throw getFunctionNotfoundException(opBinding);
    }

    private UserException getFunctionNotfoundException(SqlOperatorBinding opBinding) {
      String operandTypes = "";
      for(int j = 0; j < opBinding.getOperandCount(); ++j) {
        operandTypes += opBinding.getOperandType(j).getSqlTypeName();
        if(j < opBinding.getOperandCount() - 1) {
          operandTypes += ",";
        }
      }

      return UserException
          .functionError()
          .message(String.format("%s does not support operand types (%s)",
              opBinding.getOperator().getName(),
              operandTypes))
          .build(logger);
    }
  }
}
