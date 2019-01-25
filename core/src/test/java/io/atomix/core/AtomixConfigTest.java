/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core;

import java.time.Duration;
import java.util.Arrays;

import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.MemberConfig;
import io.atomix.cluster.MembershipConfig;
import io.atomix.cluster.MulticastConfig;
import io.atomix.cluster.discovery.MulticastDiscoveryConfig;
import io.atomix.cluster.discovery.MulticastDiscoveryProvider;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.protocol.HeartbeatMembershipProtocolConfig;
import io.atomix.core.log.DistributedLogConfig;
import io.atomix.core.map.AtomicMapConfig;
import io.atomix.core.profile.ConsensusProfile;
import io.atomix.core.profile.ConsensusProfileConfig;
import io.atomix.core.profile.DataGridProfile;
import io.atomix.core.profile.DataGridProfileConfig;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.core.value.AtomicValueConfig;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocolConfig;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroup;
import io.atomix.protocols.backup.partition.PrimaryBackupPartitionGroupConfig;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.log.DistributedLogProtocolConfig;
import io.atomix.protocols.log.partition.LogPartitionGroup;
import io.atomix.protocols.log.partition.LogPartitionGroupConfig;
import io.atomix.protocols.raft.MultiRaftProtocolConfig;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;
import io.atomix.utils.memory.MemorySize;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Atomix configuration test.
 */
public class AtomixConfigTest {
  @Test
  public void testDefaultAtomixConfig() throws Exception {
    AtomixConfig config = Atomix.config();
    assertTrue(config.getPartitionGroups().isEmpty());
    assertTrue(config.getProfiles().isEmpty());
  }

  @Test
  public void testAtomixConfig() throws Exception {
    AtomixConfig config = Atomix.config(getClass().getClassLoader().getResource("test.conf").getPath());

    ClusterConfig cluster = config.getClusterConfig();
    assertEquals("test", cluster.getClusterId());

    MemberConfig node = cluster.getNodeConfig();
    assertEquals("one", node.getId().id());
    assertEquals("localhost:5000", node.getAddress().toString());
    assertEquals("foo", node.getZoneId());
    assertEquals("bar", node.getRackId());
    assertEquals("baz", node.getHostId());
    assertEquals("bar", node.getProperties().getProperty("foo"));
    assertEquals("baz", node.getProperties().getProperty("bar"));

    MulticastConfig multicast = cluster.getMulticastConfig();
    assertTrue(multicast.isEnabled());
    assertEquals("230.0.1.1", multicast.getGroup().getHostAddress());
    assertEquals(56789, multicast.getPort());

    HeartbeatMembershipProtocolConfig protocol = (HeartbeatMembershipProtocolConfig) cluster.getProtocolConfig();
    assertEquals(Duration.ofMillis(200), protocol.getHeartbeatInterval());
    assertEquals(12, protocol.getPhiFailureThreshold());
    assertEquals(Duration.ofSeconds(15), protocol.getFailureTimeout());

    MembershipConfig membership = cluster.getMembershipConfig();
    assertEquals(Duration.ofSeconds(1), membership.getBroadcastInterval());
    assertEquals(12, membership.getReachabilityThreshold());
    assertEquals(Duration.ofSeconds(15), membership.getReachabilityTimeout());

    MulticastDiscoveryConfig discovery = (MulticastDiscoveryConfig) cluster.getDiscoveryConfig();
    assertEquals(MulticastDiscoveryProvider.TYPE, discovery.getType());
    assertEquals(Duration.ofSeconds(1), discovery.getBroadcastInterval());
    assertEquals(12, discovery.getFailureThreshold());
    assertEquals(Duration.ofSeconds(15), discovery.getFailureTimeout());

    MessagingConfig messaging = cluster.getMessagingConfig();
    assertEquals(2, messaging.getInterfaces().size());
    assertEquals("127.0.0.1", messaging.getInterfaces().get(0));
    assertEquals("0.0.0.0", messaging.getInterfaces().get(1));
    assertEquals(5000, messaging.getPort().intValue());
    assertEquals(Duration.ofSeconds(10), messaging.getConnectTimeout());
    assertTrue(messaging.getTlsConfig().isEnabled());
    assertEquals("keystore.jks", messaging.getTlsConfig().getKeyStore());
    assertEquals("foo", messaging.getTlsConfig().getKeyStorePassword());
    assertEquals("truststore.jks", messaging.getTlsConfig().getTrustStore());
    assertEquals("bar", messaging.getTlsConfig().getTrustStorePassword());

    RaftPartitionGroupConfig managementGroup = (RaftPartitionGroupConfig) config.getManagementGroup();
    assertEquals(RaftPartitionGroup.TYPE, managementGroup.getType());
    assertEquals(1, managementGroup.getPartitions());
    assertEquals(new MemorySize(1024 * 1024 * 16), managementGroup.getStorageConfig().getSegmentSize());

    RaftPartitionGroupConfig groupOne = (RaftPartitionGroupConfig) config.getPartitionGroups().get("one");
    assertEquals(RaftPartitionGroup.TYPE, groupOne.getType());
    assertEquals("one", groupOne.getName());
    assertEquals(7, groupOne.getPartitions());

    PrimaryBackupPartitionGroupConfig groupTwo = (PrimaryBackupPartitionGroupConfig) config.getPartitionGroups().get("two");
    assertEquals(PrimaryBackupPartitionGroup.TYPE, groupTwo.getType());
    assertEquals("two", groupTwo.getName());
    assertEquals(32, groupTwo.getPartitions());

    LogPartitionGroupConfig groupThree = (LogPartitionGroupConfig) config.getPartitionGroups().get("three");
    assertEquals(LogPartitionGroup.TYPE, groupThree.getType());
    assertEquals("three", groupThree.getName());
    assertEquals(3, groupThree.getPartitions());

    ConsensusProfileConfig consensusProfile = (ConsensusProfileConfig) config.getProfiles().get(0);
    assertEquals(ConsensusProfile.TYPE, consensusProfile.getType());
    assertEquals("management", consensusProfile.getManagementGroup());
    assertEquals("consensus", consensusProfile.getDataGroup());
    assertEquals(3, consensusProfile.getPartitions());
    assertTrue(consensusProfile.getMembers().containsAll(Arrays.asList("one", "two", "three")));

    DataGridProfileConfig dataGridProfile = (DataGridProfileConfig) config.getProfiles().get(1);
    assertEquals(DataGridProfile.TYPE, dataGridProfile.getType());
    assertEquals("management", dataGridProfile.getManagementGroup());
    assertEquals("data", dataGridProfile.getDataGroup());
    assertEquals(32, dataGridProfile.getPartitions());

    AtomicMapConfig fooDefaults = config.getPrimitiveDefault("atomic-map");
    assertEquals("atomic-map", fooDefaults.getType().name());
    assertEquals("two", ((MultiPrimaryProtocolConfig) fooDefaults.getProtocolConfig()).getGroup());

    AtomicMapConfig foo = config.getPrimitive("foo");
    assertEquals("atomic-map", foo.getType().name());
    assertTrue(foo.isNullValues());

    DistributedSetConfig bar = config.getPrimitive("bar");
    assertTrue(bar.getCacheConfig().isEnabled());

    MultiPrimaryProtocolConfig multiPrimary = (MultiPrimaryProtocolConfig) bar.getProtocolConfig();
    assertEquals(MultiPrimaryProtocol.TYPE, multiPrimary.getType());
    assertEquals(Replication.SYNCHRONOUS, multiPrimary.getReplication());
    assertEquals(Duration.ofSeconds(1), multiPrimary.getRetryDelay());

    AtomicValueConfig baz = config.getPrimitive("baz");

    MultiRaftProtocolConfig multiRaft = (MultiRaftProtocolConfig) baz.getProtocolConfig();
    assertEquals(ReadConsistency.SEQUENTIAL, multiRaft.getReadConsistency());
    assertEquals(Recovery.RECOVER, multiRaft.getRecoveryStrategy());
    assertEquals(Duration.ofSeconds(2), multiRaft.getRetryDelay());

    DistributedLogConfig log = config.getPrimitive("log");
    assertEquals("log", log.getType().name());

    DistributedLogProtocolConfig logConfig = (DistributedLogProtocolConfig) log.getProtocolConfig();
    assertEquals(DistributedLogProtocol.TYPE, logConfig.getType());
    assertEquals("three", logConfig.getGroup());
  }
}
