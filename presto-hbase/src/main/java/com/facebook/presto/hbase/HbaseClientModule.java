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
package com.facebook.presto.hbase;

import com.facebook.presto.hbase.common.HbaseConfig;
import com.facebook.presto.hbase.common.HbaseConnectorId;
import com.facebook.presto.hbase.spi.*;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import javax.inject.Inject;
import javax.inject.Provider;

import static com.facebook.presto.hbase.common.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class HbaseClientModule implements Module {

    private final String connectorId;
    private final TypeManager typeManager;

    public HbaseClientModule(String connectorId, TypeManager typeManager) {
        this.connectorId = connectorId;
        this.typeManager = typeManager;
    }

    @Override
    public void configure(Binder binder) {
        binder.bind(TypeManager.class).toInstance(typeManager);

        binder.bind(HbaseConnector.class).in(Scopes.SINGLETON);
        binder.bind(HbaseConnectorId.class).toInstance(new HbaseConnectorId(connectorId));
        binder.bind(HbaseMetadata.class).in(Scopes.SINGLETON);
        //binder.bind(HbaseMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(HbaseClient.class).in(Scopes.SINGLETON);
        binder.bind(HbaseSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(HbaseRecordSetProvider.class).in(Scopes.SINGLETON);
        //binder.bind(HbasePageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(HbaseHandleResolver.class).in(Scopes.SINGLETON);
        //binder.bind(HbaseSessionProperties.class).in(Scopes.SINGLETON);
        //binder.bind(HbaseTableProperties.class).in(Scopes.SINGLETON);
        //binder.bind(ZooKeeperMetadataManager.class).in(Scopes.SINGLETON);
        //binder.bind(HbaseTableManager.class).in(Scopes.SINGLETON);
        //binder.bind(IndexLookup.class).in(Scopes.SINGLETON);
        //binder.bind(ColumnCardinalityCache.class).in(Scopes.SINGLETON);
        binder.bind(Connection.class).toProvider(ConnectorProvider.class);

        configBinder(binder).bindConfig(HbaseConfig.class);
    }

    private static class ConnectorProvider implements Provider<Connection> {
        private static final Logger LOG = Logger.get(ConnectorProvider.class);

        private final String rootdir;
        private final String zookeepers;
        private final String username = null;
        private final String password = null;

        @Inject
        public ConnectorProvider(HbaseConfig config)
        {
            requireNonNull(config, "config is null");
            this.rootdir = config.getRootDir();
            this.zookeepers = config.getZkQuorum();
        }

        @Override
        public Connection get() {
            try {
                Configuration conf = HBaseConfiguration.create();
                conf.set(HbaseConfig.ROOTDIR, this.rootdir);
                conf.set(HbaseConfig.ZK_QUORUM, this.zookeepers);
                Connection connection = ConnectionFactory.createConnection(conf);
                LOG.info("Connection to hbase %s at %s established", rootdir, zookeepers);
                return connection;
            } catch (Exception e) {
                throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to create connection to HBase", e);
            }
        }
    }
}
