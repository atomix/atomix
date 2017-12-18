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

  private Node buildNode(int nodeId, Node.Type type) {
    return Node.builder(String.valueOf(nodeId))
        .withType(type)
        .withEndpoint(new Endpoint(localhost, nodeId))
        .build();
  }

  private ClusterMetadata buildClusterMetadata(Integer... bootstrapNodes) {
    List<Node> bootstrap = new ArrayList<>();
    for (int bootstrapNode : bootstrapNodes) {
      bootstrap.add(Node.builder(String.valueOf(bootstrapNode))
          .withType(Node.Type.DATA)
          .withEndpoint(new Endpoint(localhost, bootstrapNode))
          .build());
    }
    return ClusterMetadata.builder().withBootstrapNodes(bootstrap).build();
  }

  @Test
  public void testClusterService() throws Exception {
    TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();

    ClusterMetadata clusterMetadata = buildClusterMetadata(1, 2, 3);

    Node localNode1 = buildNode(1, Node.Type.DATA);
    ManagedClusterService clusterService1 = new DefaultClusterService(
        localNode1,
        new TestClusterMetadataService(clusterMetadata),
        messagingServiceFactory.newMessagingService(localNode1.endpoint()).start().join());

    Node localNode2 = buildNode(2, Node.Type.DATA);
    ManagedClusterService clusterService2 = new DefaultClusterService(
        localNode2,
        new TestClusterMetadataService(clusterMetadata),
        messagingServiceFactory.newMessagingService(localNode2.endpoint()).start().join());

    Node localNode3 = buildNode(3, Node.Type.DATA);
    ManagedClusterService clusterService3 = new DefaultClusterService(
        localNode3,
        new TestClusterMetadataService(clusterMetadata),
        messagingServiceFactory.newMessagingService(localNode3.endpoint()).start().join());

    assertNull(clusterService1.getNode(NodeId.from("1")));
    assertNull(clusterService1.getNode(NodeId.from("2")));
    assertNull(clusterService1.getNode(NodeId.from("3")));

    CompletableFuture<ClusterService>[] futures = new CompletableFuture[3];
    futures[0] = clusterService1.start();
    futures[1] = clusterService2.start();
    futures[2] = clusterService3.start();

    CompletableFuture.allOf(futures).join();

    Thread.sleep(1000);

    assertEquals(3, clusterService1.getNodes().size());
    assertEquals(3, clusterService2.getNodes().size());
    assertEquals(3, clusterService3.getNodes().size());

    assertEquals(Node.Type.DATA, clusterService1.getLocalNode().type());
    assertEquals(Node.Type.DATA, clusterService1.getNode(NodeId.from("1")).type());
    assertEquals(Node.Type.DATA, clusterService1.getNode(NodeId.from("2")).type());
    assertEquals(Node.Type.DATA, clusterService1.getNode(NodeId.from("3")).type());

    assertEquals(State.ACTIVE, clusterService1.getLocalNode().getState());
    assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("1")).getState());
    assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("2")).getState());
    assertEquals(State.ACTIVE, clusterService1.getNode(NodeId.from("3")).getState());

    Node clientNode = buildNode(4, Node.Type.CLIENT);

    ManagedClusterService clientClusterService = new DefaultClusterService(
        clientNode,
        new TestClusterMetadataService(clusterMetadata),
        messagingServiceFactory.newMessagingService(clientNode.endpoint()).start().join());

    assertEquals(State.INACTIVE, clientClusterService.getLocalNode().getState());

    assertNull(clientClusterService.getNode(NodeId.from("1")));
    assertNull(clientClusterService.getNode(NodeId.from("2")));
    assertNull(clientClusterService.getNode(NodeId.from("3")));
    assertNull(clientClusterService.getNode(NodeId.from("4")));

    clientClusterService.start().join();

    Thread.sleep(100);

    assertEquals(4, clusterService1.getNodes().size());
    assertEquals(4, clusterService2.getNodes().size());
    assertEquals(4, clusterService3.getNodes().size());
    assertEquals(4, clientClusterService.getNodes().size());

    assertEquals(Node.Type.CLIENT, clientClusterService.getLocalNode().type());

    assertEquals(Node.Type.DATA, clientClusterService.getNode(NodeId.from("1")).type());
    assertEquals(Node.Type.DATA, clientClusterService.getNode(NodeId.from("2")).type());
    assertEquals(Node.Type.DATA, clientClusterService.getNode(NodeId.from("3")).type());
    assertEquals(Node.Type.CLIENT, clientClusterService.getNode(NodeId.from("4")).type());

    assertEquals(State.ACTIVE, clientClusterService.getLocalNode().getState());

    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("1")).getState());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("2")).getState());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("3")).getState());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("4")).getState());

    Thread.sleep(2500);

    clusterService1.stop().join();

    Thread.sleep(2500);

    assertEquals(4, clusterService2.getNodes().size());
    assertEquals(Node.Type.DATA, clusterService2.getNode(NodeId.from("1")).type());

    assertEquals(State.INACTIVE, clusterService2.getNode(NodeId.from("1")).getState());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("2")).getState());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("3")).getState());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("4")).getState());

    assertEquals(State.INACTIVE, clientClusterService.getNode(NodeId.from("1")).getState());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("2")).getState());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("3")).getState());
    assertEquals(State.ACTIVE, clientClusterService.getNode(NodeId.from("4")).getState());

    clientClusterService.stop().join();

    Thread.sleep(2500);

    assertEquals(3, clusterService2.getNodes().size());

    assertEquals(State.INACTIVE, clusterService2.getNode(NodeId.from("1")).getState());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("2")).getState());
    assertEquals(State.ACTIVE, clusterService2.getNode(NodeId.from("3")).getState());
    assertNull(clusterService2.getNode(NodeId.from("4")));
  }
}
