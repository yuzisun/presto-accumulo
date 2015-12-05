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

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import io.airlift.json.JsonCodec;
import io.airlift.testing.EquivalenceTester;

import org.testng.annotations.Test;

public class TestAccumuloTableHandle {
    private final AccumuloTableHandle tableHandle = new AccumuloTableHandle(
            "connectorId", "schemaName", "tableName");

    @Test(enabled = false)
    public void testJsonRoundTrip() {
        JsonCodec<AccumuloTableHandle> codec = jsonCodec(
                AccumuloTableHandle.class);
        String json = codec.toJson(tableHandle);
        AccumuloTableHandle copy = codec.fromJson(json);
        assertEquals(copy, tableHandle);
    }

    @Test(enabled = false)
    public void testEquivalence() {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new AccumuloTableHandle("connector", "schema", "table"),
                        new AccumuloTableHandle("connector", "schema", "table"))
                .addEquivalentGroup(
                        new AccumuloTableHandle("connectorX", "schema",
                                "table"),
                        new AccumuloTableHandle("connectorX", "schema",
                                "table"))
                .addEquivalentGroup(
                        new AccumuloTableHandle("connector", "schemaX",
                                "table"),
                        new AccumuloTableHandle("connector", "schemaX",
                                "table"))
                .addEquivalentGroup(
                        new AccumuloTableHandle("connector", "schema",
                                "tableX"),
                        new AccumuloTableHandle("connector", "schema",
                                "tableX"))
                .check();
    }
}
