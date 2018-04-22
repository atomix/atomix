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
package io.atomix.cluster.messaging.impl;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.cluster.ClusterMetadata;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.impl.DefaultBootstrapMetadataService;
import io.atomix.cluster.impl.DefaultClusterService;
import io.atomix.cluster.impl.TestPersistentMetadataService;
import io.atomix.cluster.messaging.ClusterEventingService;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Cluster event service test.
 */
public class DefaultClusterEventingServiceTest {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC);

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

  @Test
  public void testClusterEventService() throws Exception {
    TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();
    TestBroadcastServiceFactory broadcastServiceFactory = new TestBroadcastServiceFactory();

    ClusterMetadata clusterMetadata = buildClusterMetadata(1, 1, 2, 3);

    Node localNode1 = buildNode(1, Node.Type.PERSISTENT);
    MessagingService messagingService1 = messagingServiceFactory.newMessagingService(localNode1.address()).start().join();
    ClusterService clusterService1 = new DefaultClusterService(
        localNode1,
        new DefaultBootstrapMetadataService(new ClusterMetadata(Collections.emptyList())),
        new TestPersistentMetadataService(clusterMetadata),
        messagingService1,
        broadcastServiceFactory.newBroadcastService().start().join())
        .start()
        .join();
    ClusterEventingService eventService1 = new DefaultClusterEventingService(clusterService1, messagingService1).start().join();

    Node localNode2 = buildNode(2, Node.Type.PERSISTENT);
    MessagingService messagingService2 = messagingServiceFactory.newMessagingService(localNode2.address()).start().join();
    ClusterService clusterService2 = new DefaultClusterService(
        localNode2,
        new DefaultBootstrapMetadataService(new ClusterMetadata(Collections.emptyList())),
        new TestPersistentMetadataService(clusterMetadata),
        messagingService2,
        broadcastServiceFactory.newBroadcastService().start().join())
        .start()
        .join();
    ClusterEventingService eventService2 = new DefaultClusterEventingService(clusterService2, messagingService2).start().join();

    Node localNode3 = buildNode(3, Node.Type.PERSISTENT);
    MessagingService messagingService3 = messagingServiceFactory.newMessagingService(localNode3.address()).start().join();
    ClusterService clusterService3 = new DefaultClusterService(
        localNode3,
        new DefaultBootstrapMetadataService(new ClusterMetadata(Collections.emptyList())),
        new TestPersistentMetadataService(clusterMetadata),
        messagingService3,
        broadcastServiceFactory.newBroadcastService().start().join())
        .start()
        .join();
    ClusterEventingService eventService3 = new DefaultClusterEventingService(clusterService3, messagingService3).start().join();

    Thread.sleep(100);

    Set<Integer> events = new CopyOnWriteArraySet<>();

    eventService1.<String>subscribe("test1", SERIALIZER::decode, message -> {
      assertEquals(message, "Hello world!");
      events.add(1);
    }, MoreExecutors.directExecutor()).join();

    eventService2.<String>subscribe("test1", SERIALIZER::decode, message -> {
      assertEquals(message, "Hello world!");
      events.add(2);
    }, MoreExecutors.directExecutor()).join();

    eventService2.<String>subscribe("test1", SERIALIZER::decode, message -> {
      assertEquals(message, "Hello world!");
      events.add(3);
    }, MoreExecutors.directExecutor()).join();

    eventService3.broadcast("test1", "Hello world!", SERIALIZER::encode);

    Thread.sleep(100);

    assertEquals(3, events.size());
    events.clear();

    eventService3.unicast("test1", "Hello world!");
    Thread.sleep(100);
    assertEquals(1, events.size());
    assertTrue(events.contains(3));
    events.clear();

    eventService3.unicast("test1", "Hello world!");
    Thread.sleep(100);
    assertEquals(1, events.size());
    assertTrue(events.contains(1));
    events.clear();

    eventService3.unicast("test1", "Hello world!");
    Thread.sleep(100);
    assertEquals(1, events.size());
    assertTrue(events.contains(2));
    events.clear();

    eventService3.unicast("test1", "Hello world!");
    Thread.sleep(100);
    assertEquals(1, events.size());
    assertTrue(events.contains(3));
    events.clear();

    eventService1.<String, String>subscribe("test2", SERIALIZER::decode, message -> {
      events.add(1);
      return message;
    }, SERIALIZER::encode, MoreExecutors.directExecutor()).join();
    eventService2.<String, String>subscribe("test2", SERIALIZER::decode, message -> {
      events.add(2);
      return message;
    }, SERIALIZER::encode, MoreExecutors.directExecutor()).join();

    assertEquals("Hello world!", eventService3.send("test2", "Hello world!").join());
    assertEquals(1, events.size());
    assertTrue(events.contains(1));
    events.clear();

    assertEquals("Hello world!", eventService3.send("test2", "Hello world!").join());
    assertEquals(1, events.size());
    assertTrue(events.contains(2));
    events.clear();

    assertEquals("Hello world!", eventService3.send("test2", "Hello world!").join());
    assertEquals(1, events.size());
    assertTrue(events.contains(1));
  }
}
