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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class SingleStoreConfig
{
    private boolean autoReconnect = true;
    private boolean enableParallelRead = false;
    private boolean materializedParallelRead = false;
    private boolean repartitionParallelRead = false;
    private List<String> parallelReadRepartitionColumns = ImmutableList.of();
    private Duration connectionTimeout = new Duration(10, TimeUnit.SECONDS);

    public boolean isAutoReconnect()
    {
        return autoReconnect;
    }

    @Config("singlestore.auto-reconnect")
    @LegacyConfig("memsql.auto-reconnect")
    public SingleStoreConfig setAutoReconnect(boolean autoReconnect)
    {
        this.autoReconnect = autoReconnect;
        return this;
    }

    @NotNull
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("singlestore.connection-timeout")
    @LegacyConfig("memsql.connection-timeout")
    public SingleStoreConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isEnableParallelRead()
    {
        return enableParallelRead;
    }

    @Config("singlestore.enableParallelRead")
    public SingleStoreConfig setEnableParallelRead(boolean enableParallelRead)
    {
        this.enableParallelRead = enableParallelRead;
        return this;
    }

    public boolean isMaterializedParallelRead()
    {
        return materializedParallelRead;
    }

    @Config("singlestore.parallelRead.materialized")
    public SingleStoreConfig setMaterializedParallelRead(boolean materializedParallelRead)
    {
        this.materializedParallelRead = materializedParallelRead;
        return this;
    }

    public boolean isRepartitionParallelRead()
    {
        return repartitionParallelRead;
    }

    @Config("singlestore.parallelRead.repartition")
    public SingleStoreConfig setRepartitionParallelRead(boolean repartitionParallelRead)
    {
        this.repartitionParallelRead = repartitionParallelRead;
        return this;
    }

    public List<String> getParallelReadRepartitionColumns()
    {
        return parallelReadRepartitionColumns;
    }

    @Config("singlestore.parallelRead.repartition.columns")
    public SingleStoreConfig setParallelReadRepartitionColumns(String parallelReadRepartitionColumns)
    {
        this.parallelReadRepartitionColumns = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(parallelReadRepartitionColumns);
        return this;
    }
}
