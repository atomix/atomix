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
import io.atomix.cluster.impl.DefaultClusterService;
import io.atomix.cluster.impl.TestClusterMetadataService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventsService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Cluster event service test.
 */
public class DefaultClusterEventsServiceTest {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC);
  private final InetAddress localhost;

  public DefaultClusterEventsServiceTest() {
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
  public void testClusterEventService() throws Exception {
    TestMessagingServiceFactory factory = new TestMessagingServiceFactory();

    ClusterMetadata clusterMetadata = buildClusterMetadata(1, 1, 2, 3);

    Node localNode1 = buildNode(1, Node.Type.DATA);
    MessagingService messagingService1 = factory.newMessagingService(localNode1.endpoint()).open().join();
    ClusterService clusterService1 = new DefaultClusterService(localNode1, new TestClusterMetadataService(clusterMetadata), messagingService1).open().join();
    ClusterCommunicationService clusterCommunicator1 = new DefaultClusterCommunicationService(clusterService1, messagingService1).open().join();
    ClusterEventsService eventService1 = new DefaultClusterEventsService(clusterService1, clusterCommunicator1).open().join();

    Node localNode2 = buildNode(2, Node.Type.DATA);
    MessagingService messagingService2 = factory.newMessagingService(localNode2.endpoint()).open().join();
    ClusterService clusterService2 = new DefaultClusterService(localNode2, new TestClusterMetadataService(clusterMetadata), messagingService2).open().join();
    ClusterCommunicationService clusterCommunicator2 = new DefaultClusterCommunicationService(clusterService2, messagingService2).open().join();
    ClusterEventsService eventService2 = new DefaultClusterEventsService(clusterService2, clusterCommunicator2).open().join();

    Node localNode3 = buildNode(3, Node.Type.DATA);
    MessagingService messagingService3 = factory.newMessagingService(localNode3.endpoint()).open().join();
    ClusterService clusterService3 = new DefaultClusterService(localNode3, new TestClusterMetadataService(clusterMetadata), messagingService3).open().join();
    ClusterCommunicationService clusterCommunicator3 = new DefaultClusterCommunicationService(clusterService3, messagingService3).open().join();
    ClusterEventsService eventService3 = new DefaultClusterEventsService(clusterService3, clusterCommunicator3).open().join();

    Thread.sleep(100);

    AtomicReference<String> value1 = new AtomicReference<>();
    eventService1.<String>addSubscriber(new MessageSubject("test1"), SERIALIZER::decode, message -> {
      value1.set(message);
    }, MoreExecutors.directExecutor()).join();

    AtomicReference<String> value2 = new AtomicReference<>();
    eventService2.<String>addSubscriber(new MessageSubject("test1"), SERIALIZER::decode, message -> {
      value2.set(message);
    }, MoreExecutors.directExecutor()).join();

    eventService3.broadcast(new MessageSubject("test1"), "Hello world!", SERIALIZER::encode);

    Thread.sleep(100);

    assertEquals("Hello world!", value1.get());
    assertEquals("Hello world!", value2.get());

    value1.set(null);
    value2.set(null);

    eventService3.unicast(new MessageSubject("test1"), "Hello world again!");
    Thread.sleep(100);
    assertEquals("Hello world again!", value2.get());
    assertNull(value1.get());
    value2.set(null);

    eventService3.unicast(new MessageSubject("test1"), "Hello world again!");
    Thread.sleep(100);
    assertEquals("Hello world again!", value1.get());
    assertNull(value2.get());
    value1.set(null);

    eventService3.unicast(new MessageSubject("test1"), "Hello world again!");
    Thread.sleep(100);
    assertEquals("Hello world again!", value2.get());
    assertNull(value1.get());
    value2.set(null);

    eventService1.<String, String>addSubscriber(new MessageSubject("test2"), SERIALIZER::decode, message -> {
      value1.set(message);
      return message;
    }, SERIALIZER::encode, MoreExecutors.directExecutor()).join();
    eventService2.<String, String>addSubscriber(new MessageSubject("test2"), SERIALIZER::decode, message -> {
      value2.set(message);
      return message;
    }, SERIALIZER::encode, MoreExecutors.directExecutor()).join();

    assertEquals("Hello world!", eventService3.sendAndReceive(new MessageSubject("test2"), "Hello world!").join());
    assertEquals("Hello world!", value2.get());
    assertNull(value1.get());
    value2.set(null);

    assertEquals("Hello world!", eventService3.sendAndReceive(new MessageSubject("test2"), "Hello world!").join());
    assertEquals("Hello world!", value1.get());
    assertNull(value2.get());
    value1.set(null);

    assertEquals("Hello world!", eventService3.sendAndReceive(new MessageSubject("test2"), "Hello world!").join());
    assertEquals("Hello world!", value2.get());
    assertNull(value1.get());
  }
}
