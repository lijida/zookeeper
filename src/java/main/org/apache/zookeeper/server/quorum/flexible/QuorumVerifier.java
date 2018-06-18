/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum.flexible;

import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;

import java.util.Map;
import java.util.Set;

/**
 * All quorum validators have to implement a method called
 * containsQuorum, which verifies if a HashSet of server
 * identifiers constitutes a quorum.
 * <p>
 * 该接口的所有实现必须实现{@link #containsQuorum(Set)},此方法判断一个服务器集合是否可以组成quorum
 * <p>
 * QuorumVerifier作为集群验证器，主要完成判断一组server在已给定的配置的server列表中，是否能够构成集群
 */
public interface QuorumVerifier {
    long getWeight(long id);

    /**
     * @param set 集群中的部分机器的标识符
     * @return true如果set可以组成quorum
     */
    boolean containsQuorum(Set<Long> set);

    long getVersion();

    void setVersion(long ver);

    Map<Long, QuorumServer> getAllMembers();

    Map<Long, QuorumServer> getVotingMembers();

    Map<Long, QuorumServer> getObservingMembers();

    @Override
    boolean equals(Object o);

    @Override
    String toString();
}
