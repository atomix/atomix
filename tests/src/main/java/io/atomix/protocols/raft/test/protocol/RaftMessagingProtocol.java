// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.test.protocol;

import io.atomix.cluster.MemberId;
import io.atomix.utils.net.Address;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.concurrent.Futures;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Messaging service based Raft protocol.
 */
public abstract class RaftMessagingProtocol {
  protected final MessagingService messagingService;
  protected final Serializer serializer;
  private final Function<MemberId, Address> addressProvider;

  public RaftMessagingProtocol(MessagingService messagingService, Serializer serializer, Function<MemberId, Address> addressProvider) {
    this.messagingService = messagingService;
    this.serializer = serializer;
    this.addressProvider = addressProvider;
  }

  protected Address address(MemberId memberId) {
    return addressProvider.apply(memberId);
  }

  protected <T, U> CompletableFuture<U> sendAndReceive(MemberId memberId, String type, T request) {
    Address address = address(memberId);
    if (address == null) {
      return Futures.exceptionalFuture(new ConnectException());
    }
    return messagingService.sendAndReceive(address, type, serializer.encode(request))
        .thenApply(serializer::decode);
  }

  protected CompletableFuture<Void> sendAsync(MemberId memberId, String type, Object request) {
    Address address = address(memberId);
    if (address != null) {
      return messagingService.sendAsync(address(memberId), type, serializer.encode(request));
    }
    return CompletableFuture.completedFuture(null);
  }

  protected <T, U> void registerHandler(String type, Function<T, CompletableFuture<U>> handler) {
    messagingService.registerHandler(type, (e, p) -> {
      CompletableFuture<byte[]> future = new CompletableFuture<>();
      handler.apply(serializer.decode(p)).whenComplete((result, error) -> {
        if (error == null) {
          future.complete(serializer.encode(result));
        } else {
          future.completeExceptionally(error);
        }
      });
      return future;
    });
  }

  protected void unregisterHandler(String type) {
    messagingService.unregisterHandler(type);
  }
}
