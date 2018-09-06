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
import com.facebook.presto.hbase.spi.HbaseColumnHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import io.airlift.log.Logger;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.hbase.common.HbaseErrorCode.*;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class HbaseClient {

    private static final Logger LOG = Logger.get(HbaseClient.class);

    private Connection connection;
    private HbaseConfig hbaseConfig;

    @Inject
    public HbaseClient(Connection connection, HbaseConfig hbaseConfig) {
        this.connection = connection;
        this.hbaseConfig = hbaseConfig;
    }

    public Set<String> getSchemas() {
        try {
            return Arrays.stream(connection.getAdmin().listNamespaceDescriptors())
                    .map(ns -> ns.getName())
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new PrestoException(ADMIN_IO_ERROR, "Failed to get admin from hbase", e);
        }
    }

    public Set<String> getTableNames(String schema) {
        try {
            return Arrays.stream(connection.getAdmin().listTableNamesByNamespace(schema))
                    .map(ns -> ns.getNameAsString())
                    .collect(Collectors.toSet());
        } catch (IOException e) {
            throw new PrestoException(ADMIN_IO_ERROR, "Failed to get admin from hbase", e);
        }
    }

    public Optional<Table> getTable(String name) {
        try {
            HTableDescriptor htb = connection.getAdmin().getTableDescriptor(TableName.valueOf(name));
            htb.getNameAsString();
            htb.getTableName().getNamespaceAsString();
            return Optional.of(connection.getTable(TableName.valueOf(name)));
        } catch (IOException e) {
            throw new PrestoException(GET_TABLE_IO_ERROR, "Failed to get table from hbase", e);
        }
    }

    public Optional<Table> getTable(SchemaTableName st) {
        return this.getTable(st.getTableName());
    }

    public List<HRegionInfo> getHRegionInfos(String name) {
        try {
            return connection.getRegionLocator(TableName.valueOf(name))
                    .getAllRegionLocations()
                    .stream()
                    .map(hrl -> hrl.getRegionInfo())
                    .collect(toImmutableList());
        } catch (IOException e) {
            throw new PrestoException(REGION_LOCATOR_ERROR, "Failed to get regionlocators from hbase", e);
        }
    }

    public HbaseTable createTable(ConnectorTableMetadata meta) {
        // Validate the DDL is something we can handle
        //validateCreateTable(meta);

        Map<String, Object> tableProperties = meta.getProperties();

        // Get the list of column handles
        List<HbaseColumnHandle> columns = getColumnHandles(meta);

        // Create the HbaseTable object
        HbaseTable table = new HbaseTable(
                meta.getTable().getSchemaName(),
                meta.getTable().getTableName(),
                columns,
                null);

        // Make sure the namespace exists
        //tableManager.ensureNamespace(table.getSchema());

        // Create the Hbase table if it does not exist (for 'external' table)
        /*if (!tableManager.exists(table.getFullTableName())) {
            tableManager.createHbaseTable(table.getFullTableName());
        }*/

        // Set any locality groups on the data table
        //setLocalityGroups(tableProperties, table);

        // Create index tables, if appropriate
        //createIndexTables(table);

        return table;
    }

    private HbaseTable toHbaseTable(ConnectorTableMetadata meta, HTableDescriptor htb) {
        TableName tb = htb.getTableName();
        return new HbaseTable(tb.getNamespaceAsString(),
                tb.getNameAsString(),
                getColumnHandles(meta),
                null);
    }

    private static List<HbaseColumnHandle> getColumnHandles(ConnectorTableMetadata meta) {
        return null;
    }

}
