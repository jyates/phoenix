/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.wal.IndexedKeyValue;
import org.junit.Test;

public class TestIndexedKeyValue {

    /**
     * The replication log log reader looks at each keyvalue that will be written to the WAL and
     * updates the replication scope on each KV. This test ensures that we correctly handle the
     * cases for an {@link IndexedKeyValue} stored in the {@link WALEdit}.
     */
    @Test
    public void testReplicationLogReader() {
        Replication replication = new Replication();
        byte[] table = Bytes.toBytes("table");
        byte[] index = Bytes.toBytes("index");
        HTableDescriptor desc = new HTableDescriptor(table);
        byte[] family = Bytes.toBytes("fam");
        HColumnDescriptor col = new HColumnDescriptor(family);
        desc.addFamily(col);

        WALEdit edit = new WALEdit();
        //add original key value
        KeyValue kv =
                new KeyValue(Bytes.toBytes("row"), family, Bytes.toBytes("qual"), 1,
                        Bytes.toBytes("val"));
        edit.add(kv);
        
        Put indexUpdate = new Put(Bytes.toBytes("index_row"));
        indexUpdate.add(family, Bytes.toBytes("index_qual"), Bytes.toBytes("index_val"));

        IndexedKeyValue ikv = new IndexedKeyValue(index, indexUpdate);
        edit.add(ikv);

        // ensure that we can update the edit for replication
        replication.visitLogEntryBeforeWrite(desc, null, edit);

        // also check that we can handle the case where the indexKV has a different Cf
        edit = new WALEdit();
        edit.add(kv);
        indexUpdate = new Put(Bytes.toBytes("index_row"));
        indexUpdate.add(Bytes.toBytes("different-index-cf"), Bytes.toBytes("index_qual"),
            Bytes.toBytes("index_val"));
        ikv = new IndexedKeyValue(index, indexUpdate);
        edit.add(ikv);

        // ensure that we can update the edit for replication
        replication.visitLogEntryBeforeWrite(desc, null, edit);
    }
}