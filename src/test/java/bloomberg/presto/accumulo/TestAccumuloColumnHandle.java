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

import io.airlift.testing.EquivalenceTester;

import org.testng.annotations.Test;

import bloomberg.presto.accumulo.AccumuloColumnHandle;
import static bloomberg.presto.accumulo.MetadataUtil.COLUMN_CODEC;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestAccumuloColumnHandle
{
    private final AccumuloColumnHandle columnHandle = new AccumuloColumnHandle("connectorId", "columnName", VARCHAR, 0);

    @Test
    public void testJsonRoundTrip()
    {
        String json = COLUMN_CODEC.toJson(columnHandle);
        AccumuloColumnHandle copy = COLUMN_CODEC.fromJson(json);
        assertEquals(copy, columnHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new AccumuloColumnHandle("connectorId", "columnName", VARCHAR, 0),
                        new AccumuloColumnHandle("connectorId", "columnName", VARCHAR, 0),
                        new AccumuloColumnHandle("connectorId", "columnName", BIGINT, 0),
                        new AccumuloColumnHandle("connectorId", "columnName", VARCHAR, 1))
                .addEquivalentGroup(
                        new AccumuloColumnHandle("connectorIdX", "columnName", VARCHAR, 0),
                        new AccumuloColumnHandle("connectorIdX", "columnName", VARCHAR, 0),
                        new AccumuloColumnHandle("connectorIdX", "columnName", BIGINT, 0),
                        new AccumuloColumnHandle("connectorIdX", "columnName", VARCHAR, 1))
                .addEquivalentGroup(
                        new AccumuloColumnHandle("connectorId", "columnNameX", VARCHAR, 0),
                        new AccumuloColumnHandle("connectorId", "columnNameX", VARCHAR, 0),
                        new AccumuloColumnHandle("connectorId", "columnNameX", BIGINT, 0),
                        new AccumuloColumnHandle("connectorId", "columnNameX", VARCHAR, 1))
                .check();
    }
}
