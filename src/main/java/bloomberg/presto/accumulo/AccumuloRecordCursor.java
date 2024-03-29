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
package bloomberg.presto.accumulo;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import bloomberg.presto.accumulo.model.AccumuloColumnHandle;
import bloomberg.presto.accumulo.serializers.AccumuloRowSerializer;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class AccumuloRecordCursor implements RecordCursor {

    private static final Logger LOG = Logger.get(AccumuloRecordCursor.class);
    private final List<AccumuloColumnHandle> cHandles;
    private final String[] fieldToColumnName;

    private final String fieldValue = "value";
    private final long totalBytes = fieldValue.getBytes().length;
    private final Scanner scan;
    private final Iterator<Entry<Key, Value>> iterator;
    private final AccumuloRowSerializer serializer;

    public AccumuloRecordCursor(AccumuloRowSerializer serializer,
            List<AccumuloColumnHandle> cHandles, Scanner scan) {
        this.cHandles = requireNonNull(cHandles, "cHandles is null");
        this.scan = requireNonNull(scan, "scan is null");

        LOG.debug("Number of column handles is " + cHandles.size());

        // if there are no columns, or the only column is the row ID, then
        // configure a scan iterator/serializer to only return the row IDs
        if (cHandles.size() == 0
                || (cHandles.size() == 1 && cHandles.get(0).getName()
                        .equals(AccumuloMetadataManager.ROW_ID_COLUMN_NAME))) {
            this.scan.addScanIterator(new IteratorSetting(1, "firstentryiter",
                    FirstEntryInRowIterator.class));
            this.serializer = new RowOnlySerializer();
            fieldToColumnName = new String[1];
            fieldToColumnName[0] = AccumuloMetadataManager.ROW_ID_COLUMN_NAME;
        } else {
            this.serializer = requireNonNull(serializer, "serializer is null");

            Text fam = new Text(), qual = new Text();
            this.scan.addScanIterator(new IteratorSetting(1,
                    "whole-row-iterator", WholeRowIterator.class));
            fieldToColumnName = new String[cHandles.size()];

            for (int i = 0; i < cHandles.size(); ++i) {
                AccumuloColumnHandle cHandle = cHandles.get(i);
                fieldToColumnName[i] = cHandle.getName();

                if (!cHandle.getName()
                        .equals(AccumuloMetadataManager.ROW_ID_COLUMN_NAME)) {
                    LOG.debug(String.format("Set column mapping %s", cHandle));
                    serializer.setMapping(cHandle.getName(),
                            cHandle.getColumnFamily(),
                            cHandle.getColumnQualifier());

                    fam.set(cHandle.getColumnFamily());
                    qual.set(cHandle.getColumnQualifier());
                    this.scan.fetchColumn(fam, qual);
                    LOG.debug(String.format(
                            "Column %s maps to Accumulo column %s:%s",
                            cHandle.getName(), fam, qual));
                } else {
                    LOG.debug(String.format("Column %s maps to Accumulo row ID",
                            cHandle.getName()));
                }
            }
        }

        iterator = this.scan.iterator();
    }

    @Override
    public long getTotalBytes() {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes() {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        checkArgument(field < cHandles.size(), "Invalid field index");
        return cHandles.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition() {
        try {
            if (iterator.hasNext()) {
                serializer.deserialize(iterator.next());
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, e);
        }
    }

    @Override
    public boolean isNull(int field) {
        checkArgument(field < cHandles.size(), "Invalid field index");
        return serializer.isNull(fieldToColumnName[field]);
    }

    @Override
    public boolean getBoolean(int field) {
        checkFieldType(field, BOOLEAN);
        return serializer.getBoolean(fieldToColumnName[field]);
    }

    @Override
    public double getDouble(int field) {
        checkFieldType(field, DOUBLE);
        return serializer.getDouble(fieldToColumnName[field]);
    }

    @Override
    public long getLong(int field) {
        checkFieldType(field, BIGINT, DATE, TIME, TIMESTAMP);
        switch (getType(field).getDisplayName()) {
        case StandardTypes.BIGINT:
            return serializer.getLong(fieldToColumnName[field]);
        case StandardTypes.DATE:
            return serializer.getDate(fieldToColumnName[field]).getTime();
        case StandardTypes.TIME:
            return serializer.getTime(fieldToColumnName[field]).getTime();
        case StandardTypes.TIMESTAMP:
            return serializer.getTimestamp(fieldToColumnName[field]).getTime();
        default:
            throw new RuntimeException("Unsupported type " + getType(field));
        }
    }

    @Override
    public Object getObject(int field) {
        Type type = getType(field);
        checkArgument(Types.isArrayType(type) || Types.isMapType(type),
                "Expected field %s to be a type of array or map but is %s",
                field, type);

        if (Types.isArrayType(type)) {
            return serializer.getArray(fieldToColumnName[field], type);
        } else {
            return serializer.getMap(fieldToColumnName[field], type);
        }
    }

    @Override
    public Slice getSlice(int field) {
        checkFieldType(field, VARBINARY, VARCHAR);
        switch (getType(field).getDisplayName()) {
        case StandardTypes.VARBINARY:
            return Slices.wrappedBuffer(
                    serializer.getVarbinary(fieldToColumnName[field]));
        case StandardTypes.VARCHAR:
            return Slices
                    .utf8Slice(serializer.getVarchar(fieldToColumnName[field]));
        default:
            throw new RuntimeException("Unsupported type " + getType(field));
        }
    }

    @Override
    public void close() {
        scan.close();
    }

    private void checkFieldType(int field, Type... expected) {
        Type actual = getType(field);

        boolean equivalent = false;
        for (Type t : expected) {
            equivalent |= actual.equals(t);
        }

        checkArgument(equivalent,
                "Expected field %s to be a type of %s but is %s", field,
                StringUtils.join(expected, ","), actual);
    }

    private static class RowOnlySerializer implements AccumuloRowSerializer {
        private Text r = new Text();

        @Override
        public void setMapping(String name, String fam, String qual) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deserialize(Entry<Key, Value> row) throws IOException {
            row.getKey().getRow(r);
        }

        @Override
        public boolean isNull(String name) {
            return false;
        }

        @Override
        public Block getArray(String name, Type type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setArray(Text value, Type type, Block block) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBoolean(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setBoolean(Text text, Boolean value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Date getDate(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDate(Text text, Date value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDouble(Text text, Double value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLong(Text text, Long value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Block getMap(String name, Type type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMap(Text text, Type type, Block block) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Time getTime(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTime(Text text, Time value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Timestamp getTimestamp(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setTimestamp(Text text, Timestamp value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getVarbinary(String string) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setVarbinary(Text text, byte[] value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getVarchar(String string) {
            return r.toString();
        }

        @Override
        public void setVarchar(Text text, String value) {
            throw new UnsupportedOperationException();
        }
    }
}
