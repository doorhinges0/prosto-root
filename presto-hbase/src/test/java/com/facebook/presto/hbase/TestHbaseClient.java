/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hbase;

import com.facebook.presto.hbase.Model.HbaseTable;
import com.facebook.presto.hbase.common.HbaseConfig;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertNotNull;

public class TestHbaseClient {

    private final HbaseClient hbaseClient;

    public TestHbaseClient() throws Exception {
        HbaseConfig config = new HbaseConfig()
                .setRootDir(HbaseQueryRunner.ROOTDIR)
                .setZkQuorum(HbaseQueryRunner.ZOOKEEPERS);
        Connection connection = HbaseQueryRunner.getHbaseConnector();
        hbaseClient = new HbaseClient(connection, config);
    }

    @Test
    public void testCreateHbaseTable() {
        /*SchemaTableName tableName = new SchemaTableName("default", "test_create_table_empty_accumulo_column");

        try {
            List<ColumnMetadata> columns = ImmutableList.of(
                    new ColumnMetadata("id", BIGINT),
                    new ColumnMetadata("a", BIGINT),
                    new ColumnMetadata("b", BIGINT),
                    new ColumnMetadata("c", BIGINT),
                    new ColumnMetadata("d", BIGINT));

            Map<String, Object> properties = new HashMap<>();
            new HbaseTableProperties().getTableProperties().forEach(meta -> properties.put(meta.getName(), meta.getDefaultValue()));
            properties.put("external", true);
            properties.put("column_mapping", "a:a:a,b::b,c:c:,d::");
            hbaseClient.createTable(new ConnectorTableMetadata(tableName, columns, properties));
            assertNotNull(hbaseClient.getTable(tableName));
        }
        finally {
            HbaseTable table = zooKeeperMetadataManager.getTable(tableName);
            if (table != null) {
                hbaseClient.dropTable(table);
            }
        }*/
    }

}
