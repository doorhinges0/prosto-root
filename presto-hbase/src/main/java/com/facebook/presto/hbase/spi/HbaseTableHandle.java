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
package com.facebook.presto.hbase.spi;

import com.facebook.presto.hbase.HbaseRowSerializer;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.MoreObjects.toStringHelper;


public class HbaseTableHandle implements ConnectorTableHandle {

    private final String connectorId;
    private final String rowKey;
    private final String schema;
    private final String serializerClassName;
    private final String table;

    @JsonCreator
    public HbaseTableHandle(String connectorId, String rowKey, String schema, String serializerClassName, String table) {
        this.connectorId = connectorId;
        this.rowKey = rowKey;
        this.schema = schema;
        this.serializerClassName = serializerClassName;
        this.table = table;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getRowKey() {
        return rowKey;
    }

    @JsonProperty
    public String getSchema() {
        return schema;
    }

    @JsonProperty
    public String getSerializerClassName() {
        return serializerClassName;
    }

    @JsonProperty
    public String getTable() {
        return table;
    }

    @JsonIgnore
    public HbaseRowSerializer getSerializerInstance()
    {
        try {
            return (HbaseRowSerializer) Class.forName(serializerClassName).getConstructor().newInstance();
        }
        catch (Exception e) {
            throw new PrestoException(NOT_FOUND, "Configured serializer class not found", e);
        }
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schema, table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, schema, table, rowKey, serializerClassName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        HbaseTableHandle other = (HbaseTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId)
                && Objects.equals(this.schema, other.schema)
                && Objects.equals(this.table, other.table)
                && Objects.equals(this.rowKey, other.rowKey)
                && Objects.equals(this.serializerClassName, other.serializerClassName);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("schema", schema)
                .add("table", table)
                .add("rowKey", rowKey)
                .add("serializerClassName", serializerClassName)
                .toString();
    }

}
