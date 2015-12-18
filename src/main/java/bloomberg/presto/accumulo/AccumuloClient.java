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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Inject;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.HostAddress;

import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;

public class AccumuloClient {
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private static final Logger LOG = Logger.get(AccumuloClient.class);
    private ZooKeeperInstance inst = null;
    private AccumuloConfig conf = null;
    private Connector conn = null;
    private AccumuloColumnMetadataProvider colMetaProvider = null;

    @Inject
    public AccumuloClient(AccumuloConfig config,
            JsonCodec<Map<String, List<AccumuloTable>>> catalogCodec)
                    throws IOException, AccumuloException,
                    AccumuloSecurityException {
        LOG.debug("constructor");
        conf = requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        inst = new ZooKeeperInstance(config.getInstance(),
                config.getZooKeepers());
        conn = inst.getConnector(config.getUsername(),
                new PasswordToken(config.getPassword().getBytes()));

        colMetaProvider = AccumuloColumnMetadataProvider.getDefault(config);
    }

    public Set<String> getSchemaNames() {
        try {
            Set<String> schemas = new HashSet<>();
            schemas.add("default");

            // add all non-accumulo reserved namespaces
            for (String ns : conn.namespaceOperations().list()) {
                if (!ns.equals("accumulo")) {
                    schemas.add(ns);
                }
            }

            return schemas;
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");

        if (schema.equals("accumulo")) {
            throw new RuntimeException("accumulo is a reserved schema");
        }

        Set<String> tableNames = new HashSet<>();

        for (String accumuloTable : conn.tableOperations().list()) {
            LOG.debug(String.format("Scanned table %s from Accumulo",
                    accumuloTable));
            if (accumuloTable.contains(".")) {
                String[] tokens = accumuloTable.split("\\.");
                if (tokens.length == 2) {
                    if (tokens[0].equals(schema)) {
                        LOG.debug(String.format("Added table %s", tokens[1]));
                        tableNames.add(tokens[1]);
                    }
                } else {
                    throw new RuntimeException(String.format(
                            "Splits from %s is not of length two: %s",
                            accumuloTable, tokens));
                }
            } else if (schema.equals("default")) {
                // skip trace table
                if (!accumuloTable.equals("trace")) {
                    LOG.debug(String.format("Added table %s", accumuloTable));
                    tableNames.add(accumuloTable);
                }
            }
        }

        return tableNames;
    }

    public AccumuloTable getTable(String schema, String tableName) {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        return new AccumuloTable(tableName,
                colMetaProvider.getColumnMetadata(schema, tableName));
    }

    public AccumuloColumn getColumnMetadata(String schema, String table,
            ColumnMetadata column) {
        return colMetaProvider.getAccumuloColumn(schema, table,
                column.getName());
    }

    public List<TabletSplitMetadata> getTabletSplits(String schemaName,
            String tableName) {
        String fulltable = schemaName.equals("default") ? tableName
                : schemaName + '.' + tableName;
        try {
            List<TabletSplitMetadata> tabletSplits = new ArrayList<>();
            String prevSplit = null;
            for (Text tSplit : conn.tableOperations().listSplits(fulltable)) {

                // get the tablet location, host and port
                String split = tSplit.toString();
                String loc = this.getTabletLocation(fulltable, split);
                String host = HostAddress.fromString(loc).getHostText();
                int port = HostAddress.fromString(loc).getPort();

                addTabletSplitMetadata(host, port, tabletSplits, prevSplit,
                        split);
                prevSplit = split;
            }

            // last range from prevSplit to infinity
            String loc = this.getTabletLocation(fulltable, null);
            String host = HostAddress.fromString(loc).getHostText();
            int port = HostAddress.fromString(loc).getPort();
            addTabletSplitMetadata(host, port, tabletSplits, prevSplit, null);
            return tabletSplits;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void addTabletSplitMetadata(String host, int port,
            List<TabletSplitMetadata> tabletSplits, String startSplit,
            String endSplit) {
        // edge case when startSplit == endSplit, or both null, no need to split
        if (startSplit == null && endSplit == null) {
            LOG.debug("start/end splits are both null");
            addTSM(tabletSplits, new TabletSplitMetadata(null, host, port,
                    new RangeHandle(null, true, null, true)));
            return;
        } else if (startSplit != null && endSplit != null
                && startSplit.equals(endSplit)) {
            LOG.debug("start/end splits are equal");
            addTSM(tabletSplits, new TabletSplitMetadata(endSplit, host, port,
                    new RangeHandle(endSplit, true, endSplit, true)));
            return;
        }

        // we'll now split this split further based on the configuration
        // get a flag saying if split is null, as the splitter doesn't
        // support null arrays
        boolean startIsNull = startSplit == null;
        boolean endIsNull = endSplit == null;

        // set the byte arrays to empty or the split bytes
        byte[] start = startIsNull ? "".getBytes() : startSplit.getBytes();
        byte[] end = endIsNull ? "".getBytes() : endSplit.getBytes();

        // make a new array of all our split locations based on the
        // number + 1
        byte[][] finalSplits = new byte[conf.getNumSplitsPerTablet() + 1][];
        finalSplits[0] = start;
        finalSplits[finalSplits.length - 1] = end;

        // recursively split the tablet into smaller pieces, our
        // configuration enforces the
        // number of splits is a power of two
        splitHelper(finalSplits, start, end, 0, conf.getNumSplitsPerTablet());

        for (int i = 0; i < finalSplits.length; ++i) {
            System.out.println(i + " " + new String(finalSplits[i]));
        }

        // and now we'll build the ranges from the splits
        for (int i = 0; i < finalSplits.length - 1; ++i) {

            try {
                String startKey = i == 0 && startIsNull ? null
                        : new String(finalSplits[i]);
                String endKey = i == finalSplits.length - 2 && endIsNull ? null
                        : new String(finalSplits[i + 1]);

                new Range(startKey, false, endKey, true);

                addTSM(tabletSplits, new TabletSplitMetadata(endKey, host, port,
                        new RangeHandle(startKey, false, endKey, true)));
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage());
            }
        }
    }

    private void addTSM(List<TabletSplitMetadata> tabletSplits,
            TabletSplitMetadata tsm) {
        if (!tabletSplits.contains(tsm)) {
            LOG.debug("Added tablet split " + tsm);
            tabletSplits.add(tsm);
        } else {
            LOG.debug("Tablet split already in list, " + tsm);
        }
    }

    long iter = 0;

    private void splitHelper(byte[][] dest, byte[] a, byte[] b, int offset,
            int len) {
        dest[offset + (len / 2)] = Utils.average(a, b);

        System.out.println(
                String.format("%d %d %s", iter, offset, new String(a)));
        System.out.println(String.format("%d %d %s", iter, len, new String(b)));
        System.out.println(String.format("%d %d %s", iter, offset + (len / 2),
                new String(dest[offset + (len / 2)])));

        ++iter;
        if (len / 2 > 1) {
            // left side
            splitHelper(dest, a, dest[len / 2], offset, len / 2);

            // right side
            splitHelper(dest, dest[len / 2], b, len / 2, len / 2);
        }
    }

    /**
     * Scans Accumulo's metadata table to retrieve the location of a given
     * tablet
     * 
     * @param fulltable
     *            The full table name &lt;namespace&gt;.&lt;tablename&gt;, or
     *            &lt;tablename&gt; if default namespace
     * @param split
     *            The split (end-row), or null for the default split (last split
     *            in the sequence)
     * @return The hostname:port pair where the split is located
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public String getTabletLocation(String fulltable, String split)
            throws TableNotFoundException, AccumuloException,
            AccumuloSecurityException {
        String tableId = conn.tableOperations().tableIdMap().get(fulltable);
        Scanner scan = conn.createScanner("accumulo.metadata",
                conn.securityOperations()
                        .getUserAuthorizations(conf.getUsername()));

        if (split != null) {
            scan.setRange(new Range(tableId + ';' + split));
        } else {
            scan.setRange(new Range(tableId + '<'));
        }

        scan.fetchColumnFamily(new Text("loc"));

        String location = null;
        for (Entry<Key, Value> kvp : scan) {
            assert location == null;
            location = kvp.getValue().toString();
        }

        LOG.debug(String.format("Location of split %s for table %s is %s",
                split, fulltable, location));
        return location;
    }
}
