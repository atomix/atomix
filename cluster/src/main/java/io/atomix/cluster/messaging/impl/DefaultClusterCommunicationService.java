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

import com.google.common.base.Objects;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.Node;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ManagedClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.messaging.Endpoint;
import io.atomix.messaging.MessagingService;
import io.atomix.utils.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster communication service implementation.
 */
public class DefaultClusterCommunicationService implements ManagedClusterCommunicationService {

  private final Logger log = LoggerFactory.getLogger(getClass());

  protected final ClusterService cluster;
  protected final MessagingService messagingService;
  private final NodeId localNodeId;
  private final AtomicBoolean open = new AtomicBoolean();

  public DefaultClusterCommunicationService(ClusterService cluster, MessagingService messagingService) {
    this.cluster = checkNotNull(cluster, "clusterService cannot be null");
    this.messagingService = checkNotNull(messagingService, "messagingService cannot be null");
    this.localNodeId = cluster.getLocalNode().id();
  }

  @Override
  public CompletableFuture<ClusterCommunicationService> open() {
    if (open.compareAndSet(false, true)) {
      log.info("Started");
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public boolean isClosed() {
    return !isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    if (open.compareAndSet(true, false)) {
      log.info("Stopped");
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M> void broadcast(
      MessageSubject subject,
      M message,
      Function<M, byte[]> encoder) {
    multicast(subject, message, encoder, cluster.getNodes()
        .stream()
        .filter(node -> !Objects.equal(node, cluster.getLocalNode()))
        .map(Node::id)
        .collect(Collectors.toSet()));
  }

  @Override
  public <M> void broadcastIncludeSelf(
      MessageSubject subject,
      M message,
      Function<M, byte[]> encoder) {
    multicast(subject, message, encoder, cluster.getNodes()
        .stream()
        .map(Node::id)
        .collect(Collectors.toSet()));
  }

  @Override
  public <M> CompletableFuture<Void> unicast(
      MessageSubject subject,
      M message,
      Function<M, byte[]> encoder,
      NodeId toNodeId) {
    try {
      byte[] payload = new ClusterMessage(
          localNodeId,
          subject,
          encoder.apply(message)
      ).getBytes();
      return doUnicast(subject, payload, toNodeId);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public <M> void multicast(
      MessageSubject subject,
      M message,
      Function<M, byte[]> encoder,
      Set<NodeId> nodes) {
    byte[] payload = new ClusterMessage(
        localNodeId,
        subject,
        encoder.apply(message))
        .getBytes();
    nodes.forEach(nodeId -> doUnicast(subject, payload, nodeId));
  }

  @Override
  public <M, R> CompletableFuture<R> sendAndReceive(
      MessageSubject subject,
      M message,
      Function<M, byte[]> encoder,
      Function<byte[], R> decoder,
      NodeId toNodeId) {
    try {
      ClusterMessage envelope = new ClusterMessage(
          cluster.getLocalNode().id(),
          subject,
          encoder.apply(message));
      return sendAndReceive(subject, envelope.getBytes(), toNodeId).thenApply(decoder);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  private CompletableFuture<Void> doUnicast(MessageSubject subject, byte[] payload, NodeId toNodeId) {
    Node node = cluster.getNode(toNodeId);
    checkArgument(node != null, "Unknown nodeId: %s", toNodeId);
    return messagingService.sendAsync(node.endpoint(), subject.toString(), payload);
  }

  private CompletableFuture<byte[]> sendAndReceive(MessageSubject subject, byte[] payload, NodeId toNodeId) {
    Node node = cluster.getNode(toNodeId);
    checkArgument(node != null, "Unknown nodeId: %s", toNodeId);
    return messagingService.sendAndReceive(node.endpoint(), subject.toString(), payload);
  }

  @Override
  public void removeSubscriber(MessageSubject subject) {
    messagingService.unregisterHandler(subject.toString());
  }

  @Override
  public <M, R> CompletableFuture<Void> addSubscriber(MessageSubject subject,
                                                      Function<byte[], M> decoder,
                                                      Function<M, R> handler,
                                                      Function<R, byte[]> encoder,
                                                      Executor executor) {
    messagingService.registerHandler(subject.toString(),
        new InternalMessageResponder<M, R>(decoder, encoder, m -> {
          CompletableFuture<R> responseFuture = new CompletableFuture<>();
          executor.execute(() -> {
            try {
              responseFuture.complete(handler.apply(m));
            } catch (Exception e) {
              responseFuture.completeExceptionally(e);
            }
          });
          return responseFuture;
        }));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M, R> CompletableFuture<Void> addSubscriber(MessageSubject subject,
                                                      Function<byte[], M> decoder,
                                                      Function<M, CompletableFuture<R>> handler,
                                                      Function<R, byte[]> encoder) {
    messagingService.registerHandler(subject.toString(),
        new InternalMessageResponder<>(decoder, encoder, handler));
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public <M> CompletableFuture<Void> addSubscriber(MessageSubject subject,
                                                   Function<byte[], M> decoder,
                                                   Consumer<M> handler,
                                                   Executor executor) {
    messagingService.registerHandler(subject.toString(),
        new InternalMessageConsumer<>(decoder, handler),
        executor);
    return CompletableFuture.completedFuture(null);
  }

  private class InternalMessageResponder<M, R> implements BiFunction<Endpoint, byte[], CompletableFuture<byte[]>> {
    private final Function<byte[], M> decoder;
    private final Function<R, byte[]> encoder;
    private final Function<M, CompletableFuture<R>> handler;

    public InternalMessageResponder(Function<byte[], M> decoder,
                                    Function<R, byte[]> encoder,
                                    Function<M, CompletableFuture<R>> handler) {
      this.decoder = decoder;
      this.encoder = encoder;
      this.handler = handler;
    }

    @Override
    public CompletableFuture<byte[]> apply(Endpoint sender, byte[] bytes) {
      return handler.apply(decoder.apply(ClusterMessage.fromBytes(bytes).payload())).thenApply(encoder);
    }
  }

  private class InternalMessageConsumer<M> implements BiConsumer<Endpoint, byte[]> {
    private final Function<byte[], M> decoder;
    private final Consumer<M> consumer;

    public InternalMessageConsumer(Function<byte[], M> decoder, Consumer<M> consumer) {
      this.decoder = decoder;
      this.consumer = consumer;
    }

    @Override
    public void accept(Endpoint sender, byte[] bytes) {
      consumer.accept(decoder.apply(ClusterMessage.fromBytes(bytes).payload()));
    }
  }
}