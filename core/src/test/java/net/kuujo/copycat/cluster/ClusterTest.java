/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.cluster;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;

import org.testng.annotations.Test;

/**
 * Cluster test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ClusterTest {
  public void shouldInitializeCluster() {
    Cluster cluster = new Cluster("foo", "bar", "baz");
    assertEquals(cluster.localMember().id(), "foo");
    assertEquals(cluster.remoteEndpoints(), Arrays.asList("bar", "baz"));
    assertEquals(cluster.remoteMemberIds(), Arrays.asList("bar", "baz"));
    assertNotNull(cluster.remoteMember("bar"));
    assertNotNull(cluster.remoteMember("baz"));
  }

  public void testAddremoteMember() {
    Cluster cluster = new Cluster("foo", "bar", "baz");
    cluster.addRemoteMember("foobarbaz");
    assertTrue(cluster.remoteMemberIds().contains("foobarbaz"));
    assertTrue(cluster.remoteEndpoints().contains("foobarbaz"));
    assertNotNull(cluster.remoteMember("foobarbaz"));
  }

  public void testRemoveremoteMember() {
    Cluster cluster = new Cluster("foo", "bar", "baz");
    cluster.removeRemoteMember("bar");
    assertFalse(cluster.remoteMemberIds().contains("bar"));
    assertFalse(cluster.remoteEndpoints().contains("bar"));
    assertNull(cluster.remoteMember("bar"));
  }

  public void shouldSyncWithNewNodes() {
    Cluster cluster = new Cluster("foo", "bar", "baz");
    Cluster cluster1 = new Cluster("foo", "barbaz");
    cluster.syncWith(cluster1);

    assertEquals(cluster1.remoteMembers(), cluster.remoteMembers());
    assertEquals(cluster1.remoteMemberIds(), cluster.remoteMemberIds());
    assertEquals(cluster1.remoteEndpoints(), cluster.remoteEndpoints());
  }

  public void shouldCopy() {
    Cluster cluster = new Cluster("foo", "bar", "baz");
    Cluster copy = cluster.copy();
    assertNotSame(copy, cluster);
    assertEquals(cluster.localMember(), copy.localMember());
    assertEquals(cluster.remoteMembers(), copy.remoteMembers());
  }
}
