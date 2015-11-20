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

import io.airlift.configuration.Config;

import java.net.URI;

import javax.validation.constraints.NotNull;

public class AccumuloConfig {
    private URI metadata;

    @NotNull
    public URI getMetadata() {
        return metadata;
    }

    @Config("metadata-uri")
    public AccumuloConfig setMetadata(URI metadata) {
        this.metadata = metadata;
        return this;
    }
}
