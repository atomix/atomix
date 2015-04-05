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
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;

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
    assertTrue(cluster.getMembers().isEmpty());
    assertTrue(cluster.getProtocol() instanceof LocalProtocol);
    cluster.addMember(1, "local://foo");
    assertEquals(1, cluster.getMembers().size());
    assertEquals(1, cluster.getMembers().iterator().next().getId());
    assertEquals("local://foo", cluster.getMembers().iterator().next().getAddress());

    ClusterConfig copy = cluster.copy();
    assertEquals(1, copy.getMembers().size());
    assertEquals(1, copy.getMembers().iterator().next().getId());
    assertEquals("local://foo", copy.getMembers().iterator().next().getAddress());
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
  public static class TestProtocol implements Protocol {
    public TestProtocol() {
    }

    @Override
    public Protocol copy() {
      return new TestProtocol();
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
