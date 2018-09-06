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
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class HbaseColumnHandle implements ColumnHandle {

    private final Optional<String> family;
    private final Optional<String> qualifier;
    private final Type type;
    private final String comment;
    private final String name;

    @JsonCreator
    public HbaseColumnHandle(@JsonProperty("family") Optional<String> family,
                             @JsonProperty("qualifier") Optional<String> qualifier,
                             @JsonProperty("type") Type type,
                             @JsonProperty("comment") String comment,
                             @JsonProperty("name") String name) {
        this.family = family;
        this.qualifier = qualifier;
        this.type = type;
        this.comment = comment;
        this.name = name;
    }

    @JsonProperty
    public Optional<String> getFamily() {
        return family;
    }

    @JsonProperty
    public Optional<String> getQualifier() {
        return qualifier;
    }

    @JsonProperty
    public Type getType() {
        return type;
    }

    @JsonProperty
    public String getComment() {
        return comment;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonIgnore
    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(name, type, comment, false);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HbaseColumnHandle that = (HbaseColumnHandle) o;
        return Objects.equals(family, that.family) &&
                Objects.equals(qualifier, that.qualifier) &&
                Objects.equals(type, that.type) &&
                Objects.equals(comment, that.comment) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(family, qualifier, type, comment, name);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("columnFamily", family.orElse(null))
                .add("columnQualifier", qualifier.orElse(null))
                .add("type", type)
                .add("comment", comment)
                .toString();
    }

}
