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

import org.junit.Ignore;
import org.junit.Test;

public class TestTpchDistributed extends BaseTestQuery {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTpchDistributed.class);

  private static void testDistributed(final String fileName) throws Exception {
    final String query = getFile(fileName);
    test("alter session set `planner.slice_target` = 10; " + query);
  }

  @Test
  public void asdf() throws Exception{
    test("select\n" +
        "  *\n" +
        "from\n" +
        "  (select\n" +
        "    i.l_quantity as imid,\n" +
        "    sum(ss.l_extendedprice) sum_sales\n" +
        "    -- avg(sum(ss.l_extendedprice)) over (partition by i.l_quantity) avg_quarterly_sales\n" +
        "  from\n" +
        "    cp.`tpch/lineitem.parquet` as i,\n" +
        "    cp.`tpch/lineitem.parquet` as ss,\n" +
        "    cp.`tpch/lineitem.parquet` as d,\n" +
        "    cp.`tpch/lineitem.parquet` as s\n" +
        "  where\n" +
        "    ss.l_orderkey = i.l_orderkey\n" +
        "    and ss.l_receiptdate = d.l_receiptdate\n" +
        "    and ss.l_suppkey = s.l_suppkey\n" +
        "    and d.l_linenumber in (1212, 1212 + 1, 1212 + 2, 1212 + 3, 1212 + 4, 1212 + 5, 1212 + 6, 1212 + 7, 1212 + 8, 1212 + 9, 1212 + 10, 1212 + 11)\n" +
        "    and ((i.l_shipmode in ('Books', 'Children', 'Electronics')\n" +
        "      and i.l_shipinstruct in ('personal', 'portable', 'reference', 'self-help')\n" +
        "      and i.l_comment in ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))\n" +
        "    or (i.l_shipmode in ('Women', 'Music', 'Men')\n" +
        "      and i.l_shipinstruct in ('accessories', 'classical', 'fragrances', 'pants')\n" +
        "      and i.l_comment in ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')))\n" +
        "    and ss.ss_sold_date_sk between 2451911 and 2452275 -- partition key filter\n" +
        "  group by\n" +
        "    i.l_quantity,\n" +
        "    d.l_extendedprice\n" +
        "  ) tmp1");
  }

  @Test
  public void tpch01() throws Exception{
    testDistributed("queries/tpch/01.sql");
  }

  @Test
  @Ignore // DRILL-512
  public void tpch02() throws Exception{
    testDistributed("queries/tpch/02.sql");
  }

  @Test
  public void tpch03() throws Exception{
    testDistributed("queries/tpch/03.sql");
  }

  @Test
  public void tpch04() throws Exception{
    testDistributed("queries/tpch/04.sql");
  }

  @Test
  public void tpch05() throws Exception{
    testDistributed("queries/tpch/05.sql");
  }

  @Test
  public void tpch06() throws Exception{
    testDistributed("queries/tpch/06.sql");
  }

  @Test
  public void tpch07() throws Exception{
    testDistributed("queries/tpch/07.sql");
  }

  @Test
  public void tpch08() throws Exception{
    testDistributed("queries/tpch/08.sql");
  }

  @Test
  public void tpch09() throws Exception{
    testDistributed("queries/tpch/09.sql");
  }

  @Test
  public void tpch10() throws Exception{
    testDistributed("queries/tpch/10.sql");
  }

  @Test
  public void tpch11() throws Exception{
    testDistributed("queries/tpch/11.sql");
  }

  @Test
  public void tpch12() throws Exception{
    testDistributed("queries/tpch/12.sql");
  }

  @Test
  public void tpch13() throws Exception{
    testDistributed("queries/tpch/13.sql");
  }

  @Test
  public void tpch14() throws Exception{
    testDistributed("queries/tpch/14.sql");
  }

  @Test
  public void tpch15() throws Exception{
    testDistributed("queries/tpch/15.sql");
  }

  @Test
  public void tpch16() throws Exception{
    testDistributed("queries/tpch/16.sql");
  }

  @Test
  @Ignore // non-equality join
  public void tpch17() throws Exception{
    testDistributed("queries/tpch/17.sql");
  }

  @Test
  public void tpch18() throws Exception{
    testDistributed("queries/tpch/18.sql");
  }

  @Test
  @Ignore // non-equality join
  public void tpch19() throws Exception{
    testDistributed("queries/tpch/19.sql");
  }

  @Test
  public void tpch19_1() throws Exception{
    testDistributed("queries/tpch/19_1.sql");
  }

  @Test
  public void tpch20() throws Exception{
    testDistributed("queries/tpch/20.sql");
  }

  @Test
  @Ignore
  public void tpch21() throws Exception{
    testDistributed("queries/tpch/21.sql");
  }

  @Test
  @Ignore // DRILL-518
  public void tpch22() throws Exception{
    testDistributed("queries/tpch/22.sql");
  }

}
