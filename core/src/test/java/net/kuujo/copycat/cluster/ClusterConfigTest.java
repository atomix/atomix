/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.util.Configurable;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Cluster configuration test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ClusterConfigTest {

  /**
   * Tests cluster default values.
   */
  public void testClusterDefaults() throws Throwable {
    ClusterConfig cluster = new ClusterConfig();
    assertEquals(cluster.getElectionTimeout(), 500);
    assertEquals(cluster.getHeartbeatInterval(), 150);
    assertTrue(cluster.getMembers().isEmpty());
    assertTrue(cluster.getProtocol() instanceof LocalProtocol);
    cluster.addMember("local://foo");
    assertEquals(1, cluster.getMembers().size());
    assertEquals("local://foo", cluster.getMembers().iterator().next());

    ClusterConfig copy = cluster.copy();
    assertEquals(1, copy.getMembers().size());
    assertEquals("local://foo", copy.getMembers().iterator().next());
  }

  /**
   * Tests overriding cluster configuration information via namespaced resources.
   */
  public void testNamespaceOverride() throws Throwable {
    ClusterConfig cluster = new ClusterConfig("foo.bar");
    assertEquals(500, cluster.getElectionTimeout());
    assertEquals(100, cluster.getHeartbeatInterval());
  }

  /**
   * Tests overriding the protocol.
   */
  public void testProtocolOverride() throws Throwable {
    ClusterConfig cluster = new ClusterConfig();
    cluster.setProtocol(new TestProtocol());
    assertTrue(cluster.getProtocol() instanceof TestProtocol);
    ClusterConfig copy = cluster.copy();
    assertTrue(copy.getProtocol() instanceof TestProtocol);
  }

  /**
   * Test protocol
   */
  public static class TestProtocol extends AbstractProtocol {
    public TestProtocol() {
    }

    public TestProtocol(Map<String, Object> config) {
      super(config);
    }

    public TestProtocol(String... resources) {
      super(resources);
    }

    public TestProtocol(Configurable config) {
      super(config);
    }

    @Override
    public ProtocolClient createClient(URI uri) {
      return new LocalProtocolClient(uri.getAuthority(), new HashMap<>());
    }

    @Override
    public ProtocolServer createServer(URI uri) {
      return new LocalProtocolServer(uri.getAuthority(), new HashMap<>());
    }
  }

}
