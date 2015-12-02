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
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;

public class AccumuloConnector implements Connector {
    private static final Logger log = Logger.get(AccumuloConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final AccumuloMetadata metadata;
    private final AccumuloSplitManager splitManager;
    private final AccumuloRecordSetProvider recordSetProvider;
    private final AccumuloHandleResolver handleResolver;

    @Inject
    public AccumuloConnector(LifeCycleManager lifeCycleManager,
            AccumuloMetadata metadata, AccumuloSplitManager splitManager,
            AccumuloRecordSetProvider recordSetProvider,
            AccumuloHandleResolver handleResolver) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager,
                "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager,
                "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider,
                "recordSetProvider is null");
        this.handleResolver = requireNonNull(handleResolver,
                "handleResolver is null");
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return recordSetProvider;
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return handleResolver;
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
