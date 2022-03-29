// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft;

import io.atomix.primitive.Recovery;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.protocols.raft.session.CommunicationStrategy;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Multi-Raft protocol configuration.
 */
public class MultiRaftProtocolConfigTest {
  @Test
  public void testConfig() throws Exception {
    MultiRaftProtocolConfig config = new MultiRaftProtocolConfig();
    assertEquals(MultiRaftProtocol.TYPE, config.getType());
    assertNull(config.getGroup());
    assertSame(Partitioner.MURMUR3, config.getPartitioner());
    assertEquals(Duration.ofMillis(250), config.getMinTimeout());
    assertEquals(Duration.ofSeconds(30), config.getMaxTimeout());
    assertEquals(ReadConsistency.SEQUENTIAL, config.getReadConsistency());
    assertEquals(CommunicationStrategy.LEADER, config.getCommunicationStrategy());
    assertEquals(Recovery.RECOVER, config.getRecoveryStrategy());
    assertEquals(0, config.getMaxRetries());
    assertEquals(Duration.ofMillis(100), config.getRetryDelay());

    Partitioner<String> partitioner = (k, p) -> null;
    config.setGroup("test");
    config.setPartitioner(partitioner);
    config.setMinTimeout(Duration.ofSeconds(1));
    config.setMaxTimeout(Duration.ofSeconds(10));
    config.setReadConsistency(ReadConsistency.LINEARIZABLE);
    config.setCommunicationStrategy(CommunicationStrategy.ANY);
    config.setRecoveryStrategy(Recovery.CLOSE);
    config.setMaxRetries(5);
    config.setRetryDelay(Duration.ofSeconds(1));

    assertEquals("test", config.getGroup());
    assertSame(partitioner, config.getPartitioner());
    assertEquals(Duration.ofSeconds(1), config.getMinTimeout());
    assertEquals(Duration.ofSeconds(10), config.getMaxTimeout());
    assertEquals(ReadConsistency.LINEARIZABLE, config.getReadConsistency());
    assertEquals(CommunicationStrategy.ANY, config.getCommunicationStrategy());
    assertEquals(Recovery.CLOSE, config.getRecoveryStrategy());
    assertEquals(5, config.getMaxRetries());
    assertEquals(Duration.ofSeconds(1), config.getRetryDelay());
  }
}
