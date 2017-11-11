/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.ManagedClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.Node.State;
import io.atomix.cluster.Node.Type;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.impl.TestMessagingServiceFactory;
import io.atomix.messaging.Endpoint;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Default cluster service test.
 */
public class DefaultClusterServiceTest {
  private final InetAddress localhost;

  public DefaultClusterServiceTest() {
    try {
      localhost = InetAddress.getByName("127.0.0.1");
    } catch (UnknownHostException e) {
      throw new AssertionError();
    }
  }

  private ClusterMetadata buildClusterMetadata(int nodeId, int... bootstrapNodes) {
    ClusterMetadata.Builder metadataBuilder = ClusterMetadata.builder()
        .withLocalNode(Node.builder()
            .withId(NodeId.from(String.valueOf(nodeId)))
            .withEndpoint(new Endpoint(localhost, nodeId))
            .build());
    List<Node> bootstrap = new ArrayList<>();
    for (int bootstrapNode : bootstrapNodes) {
      bootstrap.add(Node.builder()
          .withId(NodeId.from(String.valueOf(bootstrapNode)))
          .withEndpoint(new Endpoint(localhost, bootstrapNode))
          .build());
    }
    return metadataBuilder.withBootstrapNodes(bootstrap).build();
  }

  @Test
  public void testClusterService() throws Exception {
    TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();

    ClusterMetadata clusterMetadata1 = buildClusterMetadata(1, 1, 2, 3);
    ManagedClusterService clusterService1 = new DefaultClusterService(
        clusterMetadata1, messagingServiceFactory.newMessagingService(clusterMetadata1.localNode().endpoint()).open().join());
    ClusterMetadata clusterMetadata2 = buildClusterMetadata(2, 1, 2, 3);
    ManagedClusterService clusterService2 = new DefaultClusterService(
        clusterMetadata2, messagingServiceFactory.newMessagingService(clusterMetadata2.localNode().endpoint()).open().join());
    ClusterMetadata clusterMetadata3 = buildClusterMetadata(3, 1, 2, 3);
    ManagedClusterService clusterService3 = new DefaultClusterService(
        clusterMetadata3, messagingServiceFactory.newMessagingService(clusterMetadata3.localNode().endpoint()).open().join());

    assertEquals(State.INACTIVE, clusterService1.getNode(NodeId.from("1")).state());
    assertEquals(State.INACTIVE, clusterService1.getNode(NodeId.from("2")).state());
    assertEquals(State.INACTIVE, clusterService1.getNode(NodeId.from("3")).state());

    CompletableFuture<ClusterService>[] futures = new CompletableFuture[3];
    futures[0] = clusterService1.open();
    futures[1] = clusterService2.open();
    futures[2] = clusterService3.open();

    CompletableFuture.allOf(futures).join();

    Thread.sleep(1000);

    assertEquals(3, clusterService1.getNodes().size());
    assertEquals(3, clusterService2.getNodes().size());
    assertEquals(3, clusterService3.getNodes().size());

    assertEquals(Type.CORE, clusterService1.getLocalNode().type());
    assertEquals(Type.CORE, clusterService1.getNode(NodeId.from("1")).type());
    assertEquals(Type.CORE, clusterService1.getNode(NodeId.from("2")).type());
    assertEquals(Type.CORE, clusterService1.getNode(NodeId.from("3")).type());

    assertEquals(State.ACTIVE, clusterService1.getLocalNode().state());
    assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("1")).state());
    assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("2")).state());
    assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("3")).state());

    ClusterMetadata clientMetadata = buildClusterMetadata(4, 1, 2, 3);

    ManagedClusterService clientClusterService = new DefaultClusterService(
        clientMetadata, messagingServiceFactory.newMessagingService(clientMetadata.localNode().endpoint()).open().join());

    assertEquals(State.INACTIVE, clientClusterService.getLocalNode().state());

    assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("1")).state());
    assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("2")).state());
    assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("3")).state());
    assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("4")).state());

    clientClusterService.open().join();

    Thread.sleep(100);

    assertEquals(4, clusterService1.getNodes().size());
    assertEquals(4, clusterService2.getNodes().size());
    assertEquals(4, clusterService3.getNodes().size());
    assertEquals(4, clientClusterService.getNodes().size());

    assertEquals(Type.CLIENT, clientClusterService.getLocalNode().type());

    assertEquals(Type.CORE, clientClusterService.getNode(NodeId.from("1")).type());
    assertEquals(Type.CORE, clientClusterService.getNode(NodeId.from("2")).type());
    assertEquals(Type.CORE, clientClusterService.getNode(NodeId.from("3")).type());
    assertEquals(Type.CLIENT, clientClusterService.getNode(NodeId.from("4")).type());

    assertEquals(State.ACTIVE, clientClusterService.getLocalNode().state());

    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("1")).state());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("2")).state());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("3")).state());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("4")).state());

    Thread.sleep(2500);

    clusterService1.close().join();

    Thread.sleep(2500);

    assertEquals(4, clusterService2.getNodes().size());
    assertEquals(Type.CORE, clusterService2.getNode(NodeId.from("1")).type());

    assertEquals(State.INACTIVE, clusterService2.getNode(NodeId.from("1")).state());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("2")).state());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("3")).state());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("4")).state());

    assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("1")).state());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("2")).state());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("3")).state());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("4")).state());

    clientClusterService.close().join();

    Thread.sleep(2500);

    assertEquals(3, clusterService2.getNodes().size());

    assertEquals(State.INACTIVE, clusterService2.getNode(NodeId.from("1")).state());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("2")).state());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("3")).state());
    assertNull(clusterService2.getNode(NodeId.from("4")));
  }
}
