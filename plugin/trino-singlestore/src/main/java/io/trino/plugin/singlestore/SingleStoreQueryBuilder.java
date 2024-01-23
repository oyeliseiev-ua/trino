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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcNamedRelationHandle;
import io.trino.plugin.jdbc.JdbcQueryRelationHandle;
import io.trino.plugin.jdbc.JdbcRelationHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

public class SingleStoreQueryBuilder extends DefaultQueryBuilder
{
    private final SingleStoreConfig singleStoreConfig;
    @Inject
    public SingleStoreQueryBuilder(
            SingleStoreConfig singleStoreConfig,
            RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
        this.singleStoreConfig = singleStoreConfig;
    }

    /**
     * Create select from result table query.
     *
     * @return select from result table query
     * @see <a href="https://docs.singlestore.com/cloud/query-data/advanced-query-topics/read-query-results-in-parallel/">Read Query Results in Parallel</a>
     */
    public PreparedQuery prepareSelectQueryFromResultTable(JdbcClient client, JdbcSplit split)
    {
        SingleStoreSplit jdcSplit = (SingleStoreSplit) split;
        ImmutableList.Builder<QueryParameter> accumulator = ImmutableList.builder();
        String sql = String.format("SELECT * FROM ::%s WHERE partition_id() = %d", client.quoted(jdcSplit.getResultTableName()), jdcSplit.getPartitionId());
        return new PreparedQuery(sql, accumulator.build());
    }

    public PreparedQuery prepareResultTable(
            String tableName,
            JdbcClient client,
            ConnectorSession session,
            Connection connection,
            JdbcRelationHandle baseRelation,
            List<JdbcColumnHandle> columns,
            TupleDomain<ColumnHandle> tupleDomain)
    {
        ImmutableList.Builder<String> conjuncts = ImmutableList.builder();
        ImmutableList.Builder<QueryParameter> accumulator = ImmutableList.builder();
        String targetSql = "SELECT " + (columns.isEmpty() ? "*" : getProjection(client, columns, ImmutableMap.of(), accumulator::add)); //todo for statistics return 1 x

        targetSql += getFrom(client, baseRelation, accumulator::add);
        toConjuncts(client, session, connection, tupleDomain, conjuncts, accumulator::add);
        List<String> clauses = conjuncts.build();
        if (!clauses.isEmpty()) {
            targetSql += " WHERE " + Joiner.on(" AND ").join(clauses);
        }

        RemoteTableName remoteTableName = ((JdbcNamedRelationHandle) baseRelation).getRemoteTableName();
        RemoteTableName remoteResultTableName = new RemoteTableName(remoteTableName.getCatalogName(), remoteTableName.getSchemaName(), tableName);
        String resultTableName = getRelation(client, remoteResultTableName);
        String sql = getCreateResultTableQuery(resultTableName, targetSql,
                singleStoreConfig.isMaterializedParallelRead(), singleStoreConfig.isRepartitionParallelRead(), singleStoreConfig.getParallelReadRepartitionColumns());
        return new PreparedQuery(sql, accumulator.build());
    }

    private String getFrom(JdbcClient client, JdbcRelationHandle baseRelation, Consumer<QueryParameter> accumulator)
    {
        if (baseRelation instanceof JdbcNamedRelationHandle) {
            return " FROM " + getRelation(client, ((JdbcNamedRelationHandle) baseRelation).getRemoteTableName());
        }
        if (baseRelation instanceof JdbcQueryRelationHandle) {
            PreparedQuery preparedQuery = ((JdbcQueryRelationHandle) baseRelation).getPreparedQuery();
            preparedQuery.getParameters().forEach(accumulator);
            return " FROM (" + preparedQuery.getQuery() + ") o";
        }
        throw new IllegalArgumentException("Unsupported relation: " + baseRelation);
    }

    /**
     * Create result table query for reading results in parallel.
     *
     * @param tableName result table name
     * @param query target select query
     * @param materialized to use single-read or multiple-read mode
     * @param needsRepartition rather needs repartition
     * @param repartitionColumns repartition columns
     * @return create result table query
     * @see <a href="https://docs.singlestore.com/cloud/query-data/advanced-query-topics/read-query-results-in-parallel/">Read Query Results in Parallel</a>
     */
    private String getCreateResultTableQuery(String tableName, String query, boolean materialized, boolean needsRepartition, List<String> repartitionColumns)
    {
        String materializedStr = materialized ? "MATERIALIZED" : "";
        if (needsRepartition) {
            String randColName = String.format("randColumn%s", UUID.randomUUID().toString().replace("-", ""));
            if (repartitionColumns.isEmpty()) {
                return String.format("CREATE %s RESULT TABLE %s PARTITION BY (%s) AS SELECT *, RAND() AS %s FROM (%s)", materializedStr, tableName, randColName, randColName, query);
            }
            else {
                return String.format("CREATE %s RESULT TABLE %s PARTITION BY (%s) AS SELECT * FROM (%s)", materializedStr, tableName, String.join(", ", repartitionColumns), query);
            }
        }
        else {
            return String.format("CREATE %s RESULT TABLE %s AS %s", materializedStr, tableName, query);
        }
    }
}
