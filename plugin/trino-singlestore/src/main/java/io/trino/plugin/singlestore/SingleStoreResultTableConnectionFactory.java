package io.trino.plugin.singlestore;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnection;
import io.trino.plugin.jdbc.JdbcQueryEventListener;
import io.trino.plugin.jdbc.LazyConnectionFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import jakarta.inject.Inject;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.RemovalCause.EXPLICIT;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SingleStoreResultTableConnectionFactory
        implements JdbcQueryEventListener
{
    private static final Logger log = Logger.get(SingleStoreResultTableConnectionFactory.class);

    @GuardedBy("this")
    private final Cache<String, Connection> resultTableConnections;
    private final Map<String, String> resultTableQueryMap;
    private final ConnectionFactory connectionFactory;

    @Inject
    public SingleStoreResultTableConnectionFactory(LazyConnectionFactory connectionFactory)
    {
        this.resultTableConnections = createConnectionsCache();
        this.connectionFactory = connectionFactory;
        this.resultTableQueryMap = new HashMap<>();
    }

    // CacheBuilder.build(CacheLoader) is forbidden, because it does not support eviction for ongoing loads.
    // In this class, loading is not used and cache is used more as a map. So this is safe.
    @SuppressModernizer
    private static Cache<String, Connection> createConnectionsCache()
    {
        return CacheBuilder.newBuilder()
                .removalListener(SingleStoreResultTableConnectionFactory::onRemoval)
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMinutes(2))
                .build();
    }

    public Connection getConnectionForTable(ConnectorSession session, String resultTableName)
            throws SQLException
    {
        resultTableQueryMap.putIfAbsent(session.getQueryId(), resultTableName);
        Connection connection = getConnection(session, resultTableName);
        return new SingleStoreResultTableConnectionFactory.CachedConnection(resultTableName, connection);
    }

    private Connection getConnection(ConnectorSession session, String resultTableName)
            throws SQLException
    {
        Connection connection = resultTableConnections.asMap().remove(resultTableName);
        if (connection != null) {
            return connection;
        }
        return connectionFactory.openConnection(session);
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        log.info("Begin query " + session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        String resultTableName = resultTableQueryMap.get(session.getQueryId());
        if (resultTableName != null) {
            log.info("Clean query " + resultTableName);
            try (Connection connection = resultTableConnections.asMap().remove(resultTableName);
                    Statement statement = connection.createStatement()) {
                statement.executeQuery(String.format("DROP RESULT TABLE %s", resultTableName));
            }
            catch (SQLException e) {
                log.error("Failed to remove result table: %s", resultTableName);
                throw new TrinoException(JDBC_ERROR, e);
            } finally {
                resultTableQueryMap.remove(session.getQueryId());
            }
        }
    }

    private static void onRemoval(RemovalNotification<String, Connection> notification)
    {
        if (notification.getCause() == EXPLICIT) {
            // connection was taken from the cache
            return;
        }
        try {
            requireNonNull(notification.getValue(), "notification.getValue() is null");
            notification.getValue().close();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    final class CachedConnection
            extends ForwardingConnection
    {
        private final String resultTableName;
        private final Connection delegate;
        private volatile boolean closed;
        private volatile boolean dirty;

        private CachedConnection(String resultTableName, Connection delegate)
        {
            this.resultTableName = requireNonNull(resultTableName, "resultTableName is null");
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        protected Connection delegate()
        {
            checkState(!closed, "Connection is already closed");
            return delegate;
        }

        @Override
        public void setAutoCommit(boolean autoCommit)
                throws SQLException
        {
            dirty = true;
            super.setAutoCommit(autoCommit);
        }

        @Override
        public void setReadOnly(boolean readOnly)
                throws SQLException
        {
            dirty = true;
            super.setReadOnly(readOnly);
        }

        @Override
        public void close()
                throws SQLException
        {
            if (closed) {
                return;
            }
            closed = true;
            if (dirty) {
                delegate.close();
            }
            else if (!delegate.isClosed()) {
                resultTableConnections.put(resultTableName, delegate);
            }
        }
    }
}
