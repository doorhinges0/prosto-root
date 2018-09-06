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
package com.facebook.presto.hbase.common;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;


/**
 * File-based configuration properties for the hbase connector
 */
public class HbaseConfig {

    public static final String ROOTDIR = "hbase.rootdir";
    public static final String ZK_QUORUM = "hbase.zookeeper.quorum";

    private String rootDir;
    private String zkQuorum;

    //private static final String zkHbaseRoot = "/hbase";

    @NotNull
    public String getRootDir() {
        return rootDir;
    }

    @Config(ROOTDIR)
    @ConfigDescription("hbase rootdir")
    public HbaseConfig setRootDir(String rootDir) {
        this.rootDir = rootDir;
        return this;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    @Config(ZK_QUORUM)
    @ConfigDescription("hbase zookeeper quorum")
    public HbaseConfig setZkQuorum(String zkQuorum) {
        this.zkQuorum = zkQuorum;
        return this;
    }

}
