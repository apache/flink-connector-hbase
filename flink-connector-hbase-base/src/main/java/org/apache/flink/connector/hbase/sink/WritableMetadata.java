/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hbase.HConstants;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Writable metadata for HBase. */
public abstract class WritableMetadata<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    public abstract T read(RowData row);

    /**
     * Returns the map of metadata keys and their corresponding data types that can be consumed by
     * HBase sink for writing.
     *
     * <p>Note: All the supported writable metadata should be manually registered in it.
     */
    public static Map<String, DataType> list() {
        Map<String, DataType> metadataMap = new HashMap<>();
        metadataMap.put(TimestampMetadata.KEY, TimestampMetadata.DATA_TYPE);
        metadataMap.put(TimeToLiveMetadata.KEY, TimeToLiveMetadata.DATA_TYPE);
        return Collections.unmodifiableMap(metadataMap);
    }

    private static void validateNotNull(RowData row, int pos, String key) {
        if (row.isNullAt(pos)) {
            throw new IllegalArgumentException(
                    String.format("Writable metadata '%s' can not accept null value", key));
        }
    }

    /** Timestamp metadata for HBase. */
    public static class TimestampMetadata extends WritableMetadata<Long> {

        public static final String KEY = "timestamp";
        public static final DataType DATA_TYPE =
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable();

        private final int pos;

        public TimestampMetadata(List<String> metadataKeys, DataType physicalDataType) {
            int idx = metadataKeys.indexOf(KEY);
            this.pos = idx < 0 ? -1 : idx + physicalDataType.getLogicalType().getChildren().size();
        }

        @Override
        public Long read(RowData row) {
            if (pos < 0) {
                return HConstants.LATEST_TIMESTAMP;
            }
            validateNotNull(row, pos, KEY);
            return row.getTimestamp(pos, 3).getMillisecond();
        }
    }

    /** Time-to-live metadata for HBase. */
    public static class TimeToLiveMetadata extends WritableMetadata<Long> {

        public static final String KEY = "ttl";
        public static final DataType DATA_TYPE = DataTypes.BIGINT().nullable();

        private final int pos;

        public TimeToLiveMetadata(List<String> metadataKeys, DataType physicalDataType) {
            int idx = metadataKeys.indexOf(KEY);
            this.pos = idx < 0 ? -1 : idx + physicalDataType.getLogicalType().getChildren().size();
        }

        @Override
        public Long read(RowData row) {
            if (pos < 0) {
                return null;
            }
            validateNotNull(row, pos, KEY);
            return row.getLong(pos);
        }
    }
}
