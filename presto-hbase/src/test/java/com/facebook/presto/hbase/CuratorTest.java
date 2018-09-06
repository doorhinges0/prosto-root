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

import com.facebook.presto.spi.PrestoException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.junit.Test;

import static com.facebook.presto.hbase.common.HbaseErrorCode.ZOOKEEPER_ERROR;

public class CuratorTest {

    private static final String ZOOKEEPERS = "10.81.55.29:2181,10.80.154.187:2181,10.81.50.66:2181";
    private static final String ZK_METADATAROOT = "/hbase";
    private static final String NS_DEFAULT_SCHEMA = "default";
    private CuratorFramework curator;

    @Test
    public void test1() {
        // Create the curator client framework to use for metadata management, set at the ZK root
        curator = CuratorFrameworkFactory.newClient(ZOOKEEPERS, new RetryForever(1000));
        curator.start();

        try {
            // Create default schema should it not exist
            if (curator.checkExists().forPath(ZK_METADATAROOT) == null) {
                System.err.println("/hbase not exists!");
            }
        } catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "ZK error checking default schema", e);
        }


    }
}
