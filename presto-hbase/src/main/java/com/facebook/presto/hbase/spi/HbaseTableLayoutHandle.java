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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class HbaseTableLayoutHandle implements ConnectorTableLayoutHandle {

    private final HbaseTableHandle table;
    private final TupleDomain<ColumnHandle> constraint;

    @JsonCreator
    public HbaseTableLayoutHandle(HbaseTableHandle table, TupleDomain<ColumnHandle> constraint) {
        this.table = table;
        this.constraint = constraint;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint() {
        return constraint;
    }

    @JsonProperty
    public HbaseTableHandle getTable() {

        return table;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        HbaseTableLayoutHandle other = (HbaseTableLayoutHandle) obj;
        return Objects.equals(table, other.table)
                && Objects.equals(constraint, other.constraint);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, constraint);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("constraint", constraint)
                .toString();
    }

}
