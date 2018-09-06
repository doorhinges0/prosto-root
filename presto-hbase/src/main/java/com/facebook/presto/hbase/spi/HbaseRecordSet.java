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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import io.airlift.log.Logger;

import java.util.List;

public class HbaseRecordSet implements RecordSet {

    private static final Logger LOG = Logger.get(HbaseRecordSet.class);
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    //private final List<HbaseColumnHandle> columnHandles;
    //private final List<HbaseColumnConstraint> constraints;
    //private final List<Type> columnTypes;
    //private final HbaseRowSerializer serializer;
    //private final BatchScanner scanner;
    //private final String rowIdName;

    @Override
    public List<Type> getColumnTypes() {
        return null;
    }

    @Override
    public RecordCursor cursor() {
        return null;
    }
}
