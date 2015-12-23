package bloomberg.presto.accumulo.serializers;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.lexicoder.BytesLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;

import bloomberg.presto.accumulo.metadata.AccumuloMetadataManager;
import io.airlift.log.Logger;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class LexicoderRowSerializer implements AccumuloRowSerializer {
    public static final byte[] TRUE = new byte[] { 1 };
    public static final byte[] FALSE = new byte[] { 0 };
    private static final Logger LOG = Logger.get(LexicoderRowSerializer.class);

    private Map<String, Map<String, String>> f2q2pc = new HashMap<>();
    private Map<String, byte[]> columnValues = new HashMap<>();
    private Text rowId = new Text(), cf = new Text(), cq = new Text(),
            value = new Text();
    private static Map<Type, Lexicoder> lexicoderMap = null;

    static {
        if (lexicoderMap == null) {
            lexicoderMap = new HashMap<>();
            lexicoderMap.put(BigintType.BIGINT, new LongLexicoder());
            lexicoderMap.put(BooleanType.BOOLEAN, new BytesLexicoder());
            lexicoderMap.put(DoubleType.DOUBLE, new DoubleLexicoder());
            lexicoderMap.put(VarbinaryType.VARBINARY, new BytesLexicoder());
            lexicoderMap.put(VarcharType.VARCHAR, new StringLexicoder());
        }
    }

    @Override
    public void setMapping(String name, String fam, String qual) {
        columnValues.put(name, null);
        Map<String, String> q2pc = f2q2pc.get(fam);
        if (q2pc == null) {
            q2pc = new HashMap<>();
            f2q2pc.put(fam, q2pc);
        }

        q2pc.put(qual, name);
        LOG.debug(String.format("Added mapping for presto col %s, %s:%s", name,
                fam, qual));

    }

    @Override
    public void deserialize(Entry<Key, Value> row) throws IOException {
        columnValues.clear();

        SortedMap<Key, Value> decodedRow = WholeRowIterator
                .decodeRow(row.getKey(), row.getValue());

        decodedRow.entrySet().iterator().next().getKey().getRow(rowId);
        columnValues.put(AccumuloMetadataManager.ROW_ID_COLUMN_NAME,
                rowId.copyBytes());

        for (Entry<Key, Value> kvp : decodedRow.entrySet()) {
            kvp.getKey().getColumnFamily(cf);
            kvp.getKey().getColumnQualifier(cq);
            value.set(kvp.getValue().get());
            columnValues.put(f2q2pc.get(cf.toString()).get(cq.toString()),
                    value.copyBytes());
        }
    }

    @Override
    public boolean isNull(String name) {
        return columnValues.get(name) == null;
    }

    @Override
    public boolean getBoolean(String name) {
        return getFieldValue(name)[0] == TRUE[0] ? true : false;
    }

    @Override
    public void setBoolean(Text text, Boolean value) {
        text.set(
                getLexicoder(BooleanType.BOOLEAN).encode(value ? TRUE : FALSE));
    }

    @Override
    public Date getDate(String name) {
        return new Date((Long) (getLexicoder(DateType.DATE)
                .decode(getFieldValue(name))));
    }

    private Lexicoder getLexicoder(Type type) {
        Lexicoder l = lexicoderMap.get(type);
        if (l == null) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR,
                    "No lexicoder for type " + type);
        }
        return l;
    }

    @Override
    public void setDate(Text text, Date value) {
        text.set(getLexicoder(DateType.DATE).encode(value.getTime()));
    }

    @Override
    public double getDouble(String name) {
        return (Double) getLexicoder(DoubleType.DOUBLE)
                .decode(getFieldValue(name));
    }

    @Override
    public void setDouble(Text text, Double value) {
        text.set(getLexicoder(DoubleType.DOUBLE).encode(value));
    }

    @Override
    public long getLong(String name) {
        return (Long) getLexicoder(BigintType.BIGINT)
                .decode(getFieldValue(name));
    }

    @Override
    public void setLong(Text text, Long value) {
        text.set(getLexicoder(BigintType.BIGINT).encode(value));
    }

    @Override
    public Time getTime(String name) {
        return new Time((Long) getLexicoder(BigintType.BIGINT)
                .decode(getFieldValue(name)));
    }

    @Override
    public void setTime(Text text, Time value) {
        text.set(getLexicoder(BigintType.BIGINT).encode(value.getTime()));
    }

    @Override
    public Timestamp getTimestamp(String name) {
        return new Timestamp((Long) getLexicoder(BigintType.BIGINT)
                .decode(getFieldValue(name)));
    }

    @Override
    public void setTimestamp(Text text, Timestamp value) {
        text.set(getLexicoder(BigintType.BIGINT).encode(value.getTime()));
    }

    @Override
    public byte[] getVarbinary(String name) {
        return (byte[]) getLexicoder(VarbinaryType.VARBINARY)
                .decode(getFieldValue(name));
    }

    @Override
    public void setVarbinary(Text text, byte[] value) {
        text.set(getLexicoder(VarbinaryType.VARBINARY).encode(value));
    }

    @Override
    public String getVarchar(String name) {
        return (String) getLexicoder(VarcharType.VARCHAR)
                .decode(getFieldValue(name));
    }

    @Override
    public void setVarchar(Text text, String value) {
        text.set(getLexicoder(VarcharType.VARCHAR).encode(value));
    }

    private byte[] getFieldValue(String name) {
        return columnValues.get(name);
    }
}
