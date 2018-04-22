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
import io.atomix.cluster.ClusterMetadataEvent;
import io.atomix.cluster.ClusterMetadataEventListener;
import io.atomix.cluster.ClusterMetadataService;
import io.atomix.cluster.ManagedPersistentMetadataService;
import io.atomix.cluster.Node;
import io.atomix.cluster.messaging.impl.TestMessagingServiceFactory;
import io.atomix.utils.concurrent.Futures;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Default cluster metadata service test.
 */
public class DefaultPersistentMetadataServiceTest {
  @Test
  public void testSingleNodeBootstrap() throws Exception {
    TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();

    ClusterMetadata clusterMetadata = buildClusterMetadata(1);

    Node localNode1 = buildNode(1, Node.Type.PERSISTENT);
    ManagedPersistentMetadataService metadataService1 = new DefaultPersistentMetadataService(
        clusterMetadata, messagingServiceFactory.newMessagingService(localNode1.address()).start().join());

    metadataService1.start().join();

    assertEquals(1, metadataService1.getMetadata().nodes().size());

    Node localNode2 = buildNode(2, Node.Type.PERSISTENT);
    ManagedPersistentMetadataService metadataService2 = new DefaultPersistentMetadataService(
        clusterMetadata, messagingServiceFactory.newMessagingService(localNode2.address()).start().join());
    metadataService2.start().join();
    metadataService2.addNode(localNode2);

    assertEquals(2, metadataService2.getMetadata().nodes().size());
  }

  @Test
  public void testClusterMetadataService() throws Exception {
    TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();

    ClusterMetadata clusterMetadata = buildClusterMetadata(1, 2, 3);

    Node localNode1 = buildNode(1, Node.Type.PERSISTENT);
    ManagedPersistentMetadataService metadataService1 = new DefaultPersistentMetadataService(
        clusterMetadata, messagingServiceFactory.newMessagingService(localNode1.address()).start().join());

    Node localNode2 = buildNode(2, Node.Type.PERSISTENT);
    ManagedPersistentMetadataService metadataService2 = new DefaultPersistentMetadataService(
        clusterMetadata, messagingServiceFactory.newMessagingService(localNode2.address()).start().join());

    Node localNode3 = buildNode(3, Node.Type.PERSISTENT);
    ManagedPersistentMetadataService metadataService3 = new DefaultPersistentMetadataService(
        clusterMetadata, messagingServiceFactory.newMessagingService(localNode3.address()).start().join());

    List<CompletableFuture<ClusterMetadataService>> futures = new ArrayList<>();
    futures.add(metadataService1.start());
    futures.add(metadataService2.start());
    futures.add(metadataService3.start());
    Futures.allOf(futures).join();

    assertEquals(3, metadataService1.getMetadata().nodes().size());
    assertEquals(3, metadataService2.getMetadata().nodes().size());
    assertEquals(3, metadataService3.getMetadata().nodes().size());

    Node localNode4 = buildNode(4, Node.Type.PERSISTENT);
    ManagedPersistentMetadataService metadataService4 = new DefaultPersistentMetadataService(
        clusterMetadata, messagingServiceFactory.newMessagingService(localNode4.address()).start().join());
    metadataService4.start().join();

    assertEquals(3, metadataService4.getMetadata().nodes().size());

    TestClusterMetadataEventListener localEventListener = new TestClusterMetadataEventListener();
    metadataService4.addListener(localEventListener);

    TestClusterMetadataEventListener remoteEventListener1 = new TestClusterMetadataEventListener();
    metadataService1.addListener(remoteEventListener1);
    TestClusterMetadataEventListener remoteEventListener2 = new TestClusterMetadataEventListener();
    metadataService2.addListener(remoteEventListener2);
    TestClusterMetadataEventListener remoteEventListener3 = new TestClusterMetadataEventListener();
    metadataService3.addListener(remoteEventListener3);

    metadataService4.addNode(localNode4);
    assertEquals(4, metadataService4.getMetadata().nodes().size());
    assertEquals(4, localEventListener.event().subject().nodes().size());

    assertEquals(4, remoteEventListener1.event().subject().nodes().size());
    assertEquals(4, metadataService1.getMetadata().nodes().size());

    assertEquals(4, remoteEventListener2.event().subject().nodes().size());
    assertEquals(4, metadataService2.getMetadata().nodes().size());

    assertEquals(4, remoteEventListener3.event().subject().nodes().size());
    assertEquals(4, metadataService3.getMetadata().nodes().size());

    Node localNode5 = buildNode(5, Node.Type.PERSISTENT);
    ManagedPersistentMetadataService metadataService5 = new DefaultPersistentMetadataService(
        clusterMetadata, messagingServiceFactory.newMessagingService(localNode5.address()).start().join());
    metadataService5.start().join();
    assertEquals(4, metadataService5.getMetadata().nodes().size());
  }

  private Node buildNode(int nodeId, Node.Type type) {
    return Node.builder(String.valueOf(nodeId))
        .withType(type)
        .withAddress("localhost", nodeId)
        .build();
  }

  private ClusterMetadata buildClusterMetadata(Integer... bootstrapNodes) {
    List<Node> bootstrap = new ArrayList<>();
    for (int bootstrapNode : bootstrapNodes) {
      bootstrap.add(Node.builder(String.valueOf(bootstrapNode))
          .withType(Node.Type.PERSISTENT)
          .withAddress("localhost", bootstrapNode)
          .build());
    }
    return ClusterMetadata.builder().withNodes(bootstrap).build();
  }

  private static class TestClusterMetadataEventListener implements ClusterMetadataEventListener {
    private final BlockingQueue<ClusterMetadataEvent> queue = new ArrayBlockingQueue<>(1);

    @Override
    public void onEvent(ClusterMetadataEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public ClusterMetadataEvent event() throws InterruptedException {
      return queue.take();
    }
  }
}
