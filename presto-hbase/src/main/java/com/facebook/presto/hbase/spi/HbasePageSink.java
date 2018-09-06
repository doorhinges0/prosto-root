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

import com.facebook.presto.hbase.Model.HbaseTable;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.connector.Connector;
import io.airlift.slice.Slice;
import org.apache.hadoop.io.Text;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class HbasePageSink implements ConnectorPageSink {

    public static final Text ROW_ID_COLUMN = new Text("___ROW___");
    //private final AccumuloRowSerializer serializer;
    //private final BatchWriter writer;
    //private final Optional<Indexer> indexer;
    //private final List<HbaseColumnHandle> columns;
    private long numRows;

    public HbasePageSink(Connector connector, HbaseTable table) {

    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        return null;
    }

    @Override
    public void abort() {

    }
}
