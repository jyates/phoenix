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

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.IndexUtil;


/**
 * Class that compiles plan to generate initial data values after a DDL command for
 * index table.
 */
public class PostIndexDDLCompiler {
    private final PhoenixConnection connection;
    private final TableRef dataTableRef;

    public PostIndexDDLCompiler(PhoenixConnection connection, TableRef dataTableRef) {
        this.connection = connection;
        this.dataTableRef = dataTableRef;
    }

    public MutationPlan compile(final PTable indexTable) throws SQLException {
        /*
         * Handles:
         * 1) Populate a newly created table with contents.
         * 2) Activate the index by setting the INDEX_STATE to 
         */
        // NOTE: For first version, we would use a upsert/select to populate the new index table and
        //   returns synchronously. Creating an index on an existing table with large amount of data
        //   will as a result take a very very long time.
        //   In the long term, we should change this to an asynchronous process to populate the index
        //   that would allow the user to easily monitor the process of index creation.
        StringBuilder indexColumns = new StringBuilder();
        StringBuilder dataColumns = new StringBuilder();
        List<PColumn> dataPKColumns = dataTableRef.getTable().getPKColumns();
        PTable dataTable = dataTableRef.getTable();
        int nPKColumns = dataPKColumns.size();
        boolean isSalted = dataTable.getBucketNum() != null;
        boolean isMultiTenant = connection.getTenantId() != null && dataTable.isMultiTenant();
        int posOffset = (isSalted ? 1 : 0) + (isMultiTenant ? 1 : 0);
        for (int i = posOffset; i < nPKColumns; i++) {
            PColumn col = dataPKColumns.get(i);
            if (col.getViewConstant() == null) {
                String indexColName = IndexUtil.getIndexColumnName(col);
                dataColumns.append('"').append(col.getName()).append("\",");
                indexColumns.append('"').append(indexColName).append("\",");
            }
        }
        for (PColumnFamily family : dataTableRef.getTable().getColumnFamilies()) {
            for (PColumn col : family.getColumns()) {
                if (col.getViewConstant() == null) {
                    String indexColName = IndexUtil.getIndexColumnName(col);
                    try {
                        indexTable.getColumn(indexColName);
                        dataColumns.append('"').append(col.getFamilyName()).append("\".");
                        dataColumns.append('"').append(col.getName()).append("\",");
                        indexColumns.append('"').append(indexColName).append("\",");
                    } catch (ColumnNotFoundException e) {
                        // Catch and ignore - means that this data column is not in the index
                    }
                }
            }
        }
        dataColumns.setLength(dataColumns.length()-1);
        indexColumns.setLength(indexColumns.length()-1);
        String schemaName = dataTableRef.getTable().getSchemaName().getString();
        String tableName = indexTable.getTableName().getString();
        
        StringBuilder updateStmtStr = new StringBuilder();
        updateStmtStr.append("UPSERT /*+ NO_INDEX */ INTO ").append(schemaName.length() == 0 ? "" : '"' + schemaName + "\".").append('"').append(tableName).append("\"(")
            .append(indexColumns).append(") SELECT ").append(dataColumns).append(" FROM ")
            .append(schemaName.length() == 0 ? "" : '"' + schemaName + "\".").append('"').append(dataTableRef.getTable().getTableName().getString()).append('"');
        
        final PhoenixStatement statement = new PhoenixStatement(connection);
        return statement.compileMutation(updateStmtStr.toString());
    }

}
