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

import javax.inject.Inject;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;

import bloomberg.presto.accumulo.model.AccumuloColumnHandle;

public class AccumuloHandleResolver implements ConnectorHandleResolver {
    private final String connectorId;

    @Inject
    public AccumuloHandleResolver(AccumuloConnectorId clientId) {
        this.connectorId = requireNonNull(clientId, "clientId is null")
                .toString();
    }

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle) {
        return tableHandle instanceof AccumuloTableHandle
                && ((AccumuloTableHandle) tableHandle).getConnectorId()
                        .equals(connectorId);
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle) {
        return columnHandle instanceof AccumuloColumnHandle
                && ((AccumuloColumnHandle) columnHandle).getConnectorId()
                        .equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorInsertTableHandle tableHandle) {
        return tableHandle instanceof AccumuloTableHandle
                && ((AccumuloTableHandle) tableHandle).getConnectorId()
                        .equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorOutputTableHandle tableHandle) {
        return tableHandle instanceof AccumuloTableHandle
                && ((AccumuloTableHandle) tableHandle).getConnectorId()
                        .equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorSplit split) {
        return split instanceof AccumuloSplit
                && ((AccumuloSplit) split).getConnectorId().equals(connectorId);
    }

    @Override
    public boolean canHandle(ConnectorTableLayoutHandle handle) {
        return handle instanceof AccumuloTableLayoutHandle;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return AccumuloTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return AccumuloTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass() {
        return AccumuloTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorOutputTableHandle> getOutputTableHandleClass() {
        return AccumuloTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return AccumuloColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return AccumuloSplit.class;
    }
}
