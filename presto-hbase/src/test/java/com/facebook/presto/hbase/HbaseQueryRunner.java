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

import com.facebook.presto.Session;
import com.facebook.presto.hbase.common.HbaseConfig;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.intellij.lang.annotations.Language;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static com.facebook.presto.hbase.common.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class HbaseQueryRunner {

    private static final Logger LOG = Logger.get(HbaseQueryRunner.class);

    //暂时用EMR的集群做测试，后续会采用启动本地local cluster的方案替换
    public static final String ROOTDIR = "hdfs://emr-header-1.cluster-69396:9000/hbase";
    public static final String ZOOKEEPERS = "emr-worker-1.cluster-69396,emr-worker-2.cluster-69396,emr-header-1.cluster-69396";

    private static boolean tpchLoaded = false;
    private static Connection connection = getHbaseConnector();

    @Ignore
    @Test
    public void test1() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        LocalHBaseCluster cluster = new LocalHBaseCluster(conf);
        cluster.startup();
        Admin admin = new HBaseAdmin(conf);
        try {
            HTableDescriptor htd =
                    new HTableDescriptor(TableName.valueOf(cluster.getClass().getName()));
            admin.createTable(htd);
        } finally {
            admin.close();
        }
        cluster.shutdown();
    }

    private HbaseQueryRunner() {}

    public static synchronized DistributedQueryRunner createHbaseQueryRunner(Map<String, String> extraProperties) throws Exception {
        DistributedQueryRunner queryRunner =
                new DistributedQueryRunner(createSession(), 4, extraProperties);

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new HbasePlugin());
        Map<String, String> hbaseProperties =
                ImmutableMap.<String, String>builder()
                        .put(HbaseConfig.ROOTDIR, ROOTDIR)
                        .put(HbaseConfig.ZK_QUORUM, ZOOKEEPERS)
                        .build();

        queryRunner.createCatalog("hbase", "hbase", hbaseProperties);

        if (!tpchLoaded) {
            //copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), TpchTable.getTables());
            //connector.tableOperations().addSplits("tpch.orders", ImmutableSortedSet.of(new Text(new LexicoderRowSerializer().encode(BIGINT, 7500L))));
            tpchLoaded = true;
        }

        return queryRunner;
    }

    private static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables) {
        LOG.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, session, sourceSchema, table);
        }
        LOG.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTable(
            QueryRunner queryRunner,
            String catalog,
            Session session,
            String schema,
            TpchTable<?> table) {
        QualifiedObjectName source = new QualifiedObjectName(catalog, schema, table.getTableName());
        String target = table.getTableName();

        @Language("SQL")
        String sql;
        switch (target) {
            case "customer":
                sql = format("CREATE TABLE %s WITH (index_columns = 'mktsegment') AS SELECT * FROM %s", target, source);
                break;
            case "lineitem":
                sql = format("CREATE TABLE %s WITH (index_columns = 'quantity,discount,returnflag,shipdate,receiptdate,shipinstruct,shipmode') AS SELECT UUID() AS uuid, * FROM %s", target, source);
                break;
            case "orders":
                sql = format("CREATE TABLE %s WITH (index_columns = 'orderdate') AS SELECT * FROM %s", target, source);
                break;
            case "part":
                sql = format("CREATE TABLE %s WITH (index_columns = 'brand,type,size,container') AS SELECT * FROM %s", target, source);
                break;
            case "partsupp":
                sql = format("CREATE TABLE %s WITH (index_columns = 'partkey') AS SELECT UUID() AS uuid, * FROM %s", target, source);
                break;
            case "supplier":
                sql = format("CREATE TABLE %s WITH (index_columns = 'name') AS SELECT * FROM %s", target, source);
                break;
            default:
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", target, source);
                break;
        }

        LOG.info("Running import for %s", target, sql);
        LOG.info("%s", sql);
        long start = System.nanoTime();
        long rows = queryRunner.execute(session, sql).getUpdateCount().getAsLong();
        LOG.info("Imported %s rows for %s in %s", rows, target, nanosSince(start));
    }

    public static Session createSession() {
        return testSessionBuilder().setCatalog("hbase").setSchema("tpch").build();
    }
    

    /**
     * Gets the HbaseConnector singleton, starting the MiniHbaseCluster on initialization.
     * This singleton instance is required so all test cases access the same MiniHbaseCluster.
     *
     * @return Hbase Connection
     */
    public static Connection getHbaseConnector() {
        if (connection != null) {
            return connection;
        }

        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set(HbaseConfig.ROOTDIR, ROOTDIR);
            conf.set(HbaseConfig.ZK_QUORUM, ZOOKEEPERS);
            Connection connection = ConnectionFactory.createConnection(conf);
            LOG.info("Connection to hbase %s at %s established", ROOTDIR, ZOOKEEPERS);
            return connection;
        } catch (Exception e) {
            throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Failed to create connection to HBase", e);
        }
    }
    
}
