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

import com.facebook.presto.hbase.spi.HbaseColumnHandle;
import com.facebook.presto.hbase.spi.HbaseSplit;
import com.facebook.presto.hbase.spi.HbaseTableHandle;
import com.facebook.presto.hbase.spi.HbaseTableLayoutHandle;
import com.facebook.presto.spi.*;

public class HbaseHandleResolver implements ConnectorHandleResolver {

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return HbaseTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return HbaseTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return HbaseColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return HbaseSplit.class;
    }
}
