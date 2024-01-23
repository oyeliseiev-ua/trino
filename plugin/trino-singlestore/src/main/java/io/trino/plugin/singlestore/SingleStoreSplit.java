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

package io.trino.plugin.singlestore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

public class SingleStoreSplit extends JdbcSplit
{
    private final String resultTableName;
    private final int partitionId;

    @Override
    public SingleStoreSplit withDynamicFilter(TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        return new SingleStoreSplit(resultTableName, partitionId, dynamicFilter);
    }

    private SingleStoreSplit(String resultTableName, int partitionId, TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        super(Optional.empty(), dynamicFilter);
        this.resultTableName = resultTableName;
        this.partitionId = partitionId;
    }

    @JsonCreator
    public SingleStoreSplit(@JsonProperty("resultTableName") String resultTableName, @JsonProperty("partitionId") int partitionId)
    {
        super(Optional.empty());
        this.resultTableName = resultTableName;
        this.partitionId = partitionId;
    }

    @JsonProperty
    public int getPartitionId()
    {
        return partitionId;
    }

    @JsonProperty
    public String getResultTableName()
    {
        return resultTableName;
    }
}
