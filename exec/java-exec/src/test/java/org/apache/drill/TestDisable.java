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
package org.apache.drill;

import org.apache.drill.exec.rpc.RpcException;
import org.junit.Test;

public class TestDisable extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestExampleQueries.class);

  @Test(expected = RpcException.class) // see DRILL-985
  public void testImplicitCartesianJoin() throws Exception {
    test("select a.*,b.user_port " +
         "from cp.`employee.json` a, sys.drillbits b ");
  }

  @Test(expected = RpcException.class)
  public void testNonEqualJoin() throws Exception {
    test("select a.*,b.user_port " +
         "from cp.`employee.json` a, sys.drillbits b " +
         "where a.position_id<>b.user_port;");
  }
}