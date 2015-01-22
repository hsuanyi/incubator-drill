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
package org.apache.drill.exec.planner.sql.parser;

import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlJoin;
import org.eigenbase.sql.JoinType;
import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlLiteral;
import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.SqlDynamicParam;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.sql.util.SqlVisitor;
import org.eigenbase.sql.util.SqlShuttle;
import org.eigenbase.sql.SqlIdentifier;
import org.eigenbase.sql.SqlDataTypeSpec;

import java.util.List;
import com.google.common.collect.Lists;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class VisitingUnsupportedOperators extends SqlShuttle {
  private static List<String> disabledType = Lists.newArrayList();
  static {
    disabledType.add("TINYINT");
    disabledType.add("SMALLINT");
    disabledType.add("REAL");
    disabledType.add("MULTISET");
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    for(String strType : disabledType) {
      if(type.getTypeName().getSimple().equals(strType)) {
        throw new UnsupportedOperationException("Data Type " + strType + " is not supported");
      }
    }

    return type;
  }

  @Override
  public SqlNode visit(org.eigenbase.sql.SqlCall sqlCall) {
    // Disable unsupported Intersect
    if(sqlCall.getKind() == SqlKind.INTERSECT) {
      throw new UnsupportedOperationException(sqlCall.getOperator() + " is not supported");
    }

    // Disable unsupported Except
    if(sqlCall.getKind() == SqlKind.EXCEPT) {
      throw new UnsupportedOperationException(sqlCall.getOperator() + " is not supported");
    }

    // Disable unsupported Union
    // The if statement will block UNION only; UNION ALL will go through
    if(sqlCall.getKind() == SqlKind.UNION && sqlCall.getOperator().isName("UNION")) {
      throw new UnsupportedOperationException("UNION is not supported");
    }

    // Disable unsupported JOINs
    if(sqlCall.getKind() == SqlKind.JOIN) {
      SqlJoin join = (SqlJoin) sqlCall;
      // Block Natural Join
      if(join.isNatural()) {
        throw new UnsupportedOperationException("NATURAL JOIN is not supported");
      }

      // Block Cross Join
      if(join.getJoinType() == JoinType.CROSS) {
        throw new UnsupportedOperationException("CROSS JOIN is not supported");
      }
    }

    SqlNode sqlNode;
    if(sqlCall.getOperator().isName("CAST")) {
      sqlNode = sqlCall.getOperandList().get(1).accept(this);
    } else {
      sqlNode = sqlCall.getOperator().acceptCall(this, sqlCall);
    }

    return sqlNode;
  }
}