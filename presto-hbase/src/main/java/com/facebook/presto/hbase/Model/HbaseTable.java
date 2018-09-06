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
package com.facebook.presto.hbase.Model;

import com.facebook.presto.hbase.spi.HbaseColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Table;

import java.util.List;

public class HbaseTable {

    private final String schema;
    private final List<ColumnMetadata> columnsMetadata;
    private final List<HbaseColumnHandle> columns;
    private final String rowKey;
    private final String table;
    private final SchemaTableName schemaTableName;

    public HbaseTable(Table table) {
        this(table.getName().getNamespaceAsString(),
                null,
                null,
                table.getName().getNamespaceAsString());
    }

    @JsonCreator
    public HbaseTable(@JsonProperty("schema") String schema,
                      @JsonProperty("table") String table,
                      @JsonProperty("columns") List<HbaseColumnHandle> columns,
                      @JsonProperty("rowKey") String rowKey
                      ) {
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.rowKey = rowKey;

        // Extract the ColumnMetadata from the handles for faster access
        ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
        for (HbaseColumnHandle column : this.columns) {
            columnMetadataBuilder.add(column.getColumnMetadata());
        }

        this.columnsMetadata = columnMetadataBuilder.build();
        this.schemaTableName = new SchemaTableName(this.schema, this.table);
    }

    @JsonProperty
    public String getSchema() {
        return schema;
    }

    @JsonProperty
    public List<HbaseColumnHandle> getColumns() {
        return columns;
    }

    @JsonProperty
    public String getRowKey() {
        return rowKey;
    }

    @JsonProperty
    public String getTable() {
        return table;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName() {
        return schemaTableName;
    }

    @JsonIgnore
    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    @JsonIgnore
    public static String getFullTableName(String schema, String table)
    {
        return schema.equals("default") ? table : schema + '.' + table;
    }

    @JsonIgnore
    public static String getFullTableName(SchemaTableName tableName)
    {
        return getFullTableName(tableName.getSchemaName(), tableName.getTableName());
    }

}
