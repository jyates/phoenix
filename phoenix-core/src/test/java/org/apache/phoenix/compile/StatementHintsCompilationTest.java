/*
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
package org.apache.phoenix.compile;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.trace.util.Tracing;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


/**
 * Test compilation of various statements with hints.
 */
public class StatementHintsCompilationTest extends BaseConnectionlessQueryTest {

   private static boolean usingSkipScan(Scan scan) {
        Filter filter = scan.getFilter();
        if (filter instanceof FilterList) {
            FilterList filterList = (FilterList) filter;
            for (Filter childFilter : filterList.getFilters()) {
                if (childFilter instanceof SkipScanFilter) {
                    return true;
                }
            }
            return false;
        }
        return filter instanceof SkipScanFilter;
    }

    private static QueryPlan compileQuery(String query) throws SQLException {
        return compileQuery(query, Collections.emptyList(), null);
    }

    private static QueryPlan compileQuery(String query, List<Object> binds, Integer limit)
            throws SQLException {
        PhoenixConnection pconn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
                        .unwrap(PhoenixConnection.class);
        PhoenixPreparedStatement pstmt = new PhoenixPreparedStatement(pconn, query);
        TestUtil.bindParams(pstmt, binds);
        QueryPlan plan = pstmt.compileQuery();
        assertEquals(limit, plan.getLimit());
        return plan;
    }

    private static StatementContext compileStatement(String query) throws SQLException {
        return compileStatement(query, Collections.emptyList(), null);
    }

    private static StatementContext compileStatement(String query, List<Object> binds, Integer limit) throws SQLException {
        QueryPlan plan = compileQuery(query, binds, limit);
        return plan.getContext();
    }

    @Test
    public void testSelectForceSkipScan() throws Exception {
        String id = "000000000000001";
        // A where clause without the first column usually compiles into a range scan.
        String query = "SELECT /*+ SKIP_SCAN */ * FROM atable WHERE entity_id='" + id + "'";
        
        Scan scan = compileStatement(query).getScan();
        assertTrue("The first filter should be SkipScanFilter.", usingSkipScan(scan));
    }

    @Test
    public void testSelectForceRangeScan() throws Exception {
        String query = "SELECT /*+ RANGE_SCAN */ * FROM atable WHERE organization_id in (" +
                "'000000000000001', '000000000000002', '000000000000003', '000000000000004')";
        Scan scan = compileStatement(query).getScan();
        // Verify that it is not using SkipScanFilter.
        assertFalse("The first filter should not be SkipScanFilter.", usingSkipScan(scan));
    }
    
    @Test
    public void testSelectForceRangeScanForEH() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table eh (organization_id char(15) not null,parent_id char(15) not null, created_date date not null, entity_history_id char(15) not null constraint pk primary key (organization_id, parent_id, created_date, entity_history_id))");
        ResultSet rs = conn.createStatement().executeQuery("explain select /*+ RANGE_SCAN */ ORGANIZATION_ID, PARENT_ID, CREATED_DATE, ENTITY_HISTORY_ID from eh where ORGANIZATION_ID='111111111111111' and SUBSTR(PARENT_ID, 1, 3) = 'foo' and CREATED_DATE >= TO_DATE ('2012-11-01 00:00:00') and CREATED_DATE < TO_DATE ('2012-11-30 00:00:00') order by ORGANIZATION_ID, PARENT_ID, CREATED_DATE DESC, ENTITY_HISTORY_ID limit 100");
        assertEquals("CLIENT PARALLEL 1-WAY RANGE SCAN OVER EH ['111111111111111','foo            ','2012-11-01 00:00:00.000'] - ['111111111111111','fop            ','2012-11-30 00:00:00.000']\n" + 
                "    SERVER FILTER BY (CREATED_DATE >= '2012-11-01 00:00:00.000' AND CREATED_DATE < '2012-11-30 00:00:00.000')\n" + 
                "    SERVER TOP 100 ROWS SORTED BY [ORGANIZATION_ID, PARENT_ID, CREATED_DATE DESC, ENTITY_HISTORY_ID]\n" + 
                "CLIENT MERGE SORT",QueryUtil.getExplainPlan(rs));
    }
    
    @Test
    public void testAddTraceSingleAnnotation() throws Exception{
        String query = "SELECT /*+ TRACE_ANNOTATION(somekey=somevalue) */ * FROM atable ";
        QueryPlan plan = compileQuery(query);
        Map<String, String> annotations = Tracing.getClientAnnotations(plan.getStatement());
        assertEquals(1, annotations.size());
        assertEquals("SOMEVALUE", annotations.get("SOMEKEY"));
    }
    
    @Test
    public void testAddTraceAnnotationPreserveCase() throws Exception {
        String query = "SELECT /*+ TRACE_ANNOTATION(\"somekey=somevalue\") */ * FROM atable ";
        QueryPlan plan = compileQuery(query);
        Map<String, String> annotations = Tracing.getClientAnnotations(plan.getStatement());
        assertEquals(1, annotations.size());
        assertEquals("somevalue", annotations.get("somekey"));
    }
    
    @Test
    public void testAddMultipleAnnotations() throws Exception {
        String query =
                "SELECT /*+ " + HintNode.Hint.TRACE_ANNOTATION.name() + "(\"somekey=somevalue\") "
                        + HintNode.Hint.TRACE_ANNOTATION.name()
                        + "(keytwo=valuetwo) */ * FROM atable ";
        QueryPlan plan = compileQuery(query);
        Map<String, String> annotations = Tracing.getClientAnnotations(plan.getStatement());
        assertEquals(2, annotations.size());
        assertEquals("somevalue", annotations.get("somekey"));
        assertEquals("VALUETWO", annotations.get("KEYTWO"));
    }

    @Test
    public void testAddTraceSingleTag() throws Exception{
        String v1 = "tag value one";
        String query =
                "SELECT /*+ " + HintNode.Hint.TRACE_TAG.name() + "(" + v1 + ") */"
                        + "* FROM atable ";
        QueryPlan plan = compileQuery(query);
        List<String> tags = Tracing.getClientTags(plan.getStatement());
        assertEquals(1, tags.size());
        assertEquals(v1.toUpperCase(), tags.get(0));
    }
    
    @Test
    public void testAddMultipleTags() throws Exception{
        String v1 = "tag value one";
        String v2 = "another value";
        String query =
                "SELECT /*+ " + HintNode.Hint.TRACE_TAG.name() + "(" + v1 + ") "
                        + HintNode.Hint.TRACE_TAG.name() + "(" + v2 + ") */ * FROM atable ";
        QueryPlan plan = compileQuery(query);
        List<String> tags = Tracing.getClientTags(plan.getStatement());
        assertEquals(2, tags.size());
        assertEquals(v1.toUpperCase(), tags.get(0));
        assertEquals(v2.toUpperCase(), tags.get(1));
    }
}