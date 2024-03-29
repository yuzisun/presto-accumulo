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
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;

import java.util.Map;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;

public class AccumuloConnectorFactory implements ConnectorFactory {
    private final TypeManager typeManager;
    private final Map<String, String> optionalConfig;

    public AccumuloConnectorFactory(TypeManager typeManager,
            Map<String, String> optionalConfig) {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.optionalConfig = ImmutableMap.copyOf(
                requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public String getName() {
        return "accumulo";
    }

    @Override
    public Connector create(final String connectorId,
            Map<String, String> requiredConfig) {
        requireNonNull(requiredConfig, "requiredConfig is null");
        requireNonNull(optionalConfig, "optionalConfig is null");

        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(new JsonModule(),
                    new AccumuloModule(connectorId, typeManager));

            Injector injector = app.strictConfig().doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(AccumuloConnector.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
