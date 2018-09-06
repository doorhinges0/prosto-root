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
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

public class HbaseRecordCursor implements RecordCursor {

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return null;
    }

    @Override
    public boolean advanceNextPosition() {
        return false;
    }

    @Override
    public boolean getBoolean(int field) {
        return false;
    }

    @Override
    public long getLong(int field) {
        return 0;
    }

    @Override
    public double getDouble(int field) {
        return 0;
    }

    @Override
    public Slice getSlice(int field) {
        return null;
    }

    @Override
    public Object getObject(int field) {
        return null;
    }

    @Override
    public boolean isNull(int field) {
        return false;
    }

    @Override
    public void close() {

    }
}
