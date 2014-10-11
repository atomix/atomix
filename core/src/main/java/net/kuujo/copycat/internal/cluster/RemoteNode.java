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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.internal.protocol.AsyncProtocolClientWrapper;
import net.kuujo.copycat.protocol.*;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;
import net.kuujo.copycat.spi.protocol.AsyncProtocolClient;
import net.kuujo.copycat.spi.protocol.BaseProtocol;
import net.kuujo.copycat.spi.protocol.Protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Remote node reference.<p>
 *
 * This type provides the interface for interacting with a remote note by exposing the appripriate
 * {@link net.kuujo.copycat.spi.protocol.ProtocolClient} instance.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RemoteNode<M extends Member> extends Node<M> {
  private final AsyncProtocolClient client;

  @SuppressWarnings("unchecked")
  public RemoteNode(M member, BaseProtocol<M> protocol) {
    super(member);
    if (protocol instanceof AsyncProtocol) {
      this.client = new QueueingAsyncProtocolClient(((AsyncProtocol<M>) protocol).createClient(member));
    } else {
      this.client = new QueueingAsyncProtocolClient(new AsyncProtocolClientWrapper(((Protocol<M>) protocol).createClient(member)));
    }
  }

  /**
   * Returns the protocol client connecting to this node.
   *
   * @return The node's protocol client.
   */
  public AsyncProtocolClient client() {
    return client;
  }

  /**
   * Async protocol client that queues requests during connection.
   */
  private static class QueueingAsyncProtocolClient implements AsyncProtocolClient {
    private final AsyncProtocolClient client;
    private CompletableFuture<Void> connectFuture;
    private boolean connected;
    private List<CompletableFuture<Void>> connectFutures = new ArrayList<>(50);

    private QueueingAsyncProtocolClient(AsyncProtocolClient client) {
      this.client = client;
    }

    @Override
    public CompletableFuture<Void> connect() {
      if (connected) {
        return CompletableFuture.completedFuture(null);
      }
      if (connectFuture == null) {
        connectFuture = new CompletableFuture<>();
        client.connect().whenComplete((result, error) -> {
          CompletableFuture<Void> future = connectFuture;
          connectFuture = null;
          if (future != null) {
            if (error != null) {
              future.completeExceptionally(error);
              triggerConnectFutures(error);
            } else {
              connected = true;
              future.complete(null);
              triggerConnectFutures(null);
            }
          }
        });
      }
      return connectFuture;
    }

    /**
     * Triggers connect callbacks.
     */
    private synchronized void triggerConnectFutures(Throwable error) {
      for (CompletableFuture<Void> future : connectFutures) {
        if (error != null) {
          future.completeExceptionally(error);
        } else {
          future.complete(null);
        }
      }
      connectFutures.clear();
    }

    @Override
    public CompletableFuture<Void> close() {
      if (connectFuture != null) {
        connectFuture.completeExceptionally(new ProtocolException("Client was closed"));
        connectFuture = null;
      }
      connected = false;
      return client.close();
    }

    @Override
    public CompletableFuture<PingResponse> ping(PingRequest request) {
      if (connected) {
        return client.ping(request);
      }
      CompletableFuture<PingResponse> future = new CompletableFuture<>();
      connectFutures.add(new CompletableFuture<Void>().whenComplete((connectResult, connectError) -> {
        if (connectError != null) {
          future.completeExceptionally(connectError);
        } else {
          client.ping(request).whenComplete((pingResponse, pingError) -> {
            if (pingError != null) {
              future.completeExceptionally(pingError);
            } else {
              future.complete(pingResponse);
            }
          });
        }
      }));
      return future;
    }

    @Override
    public CompletableFuture<SyncResponse> sync(SyncRequest request) {
      if (connected) {
        return client.sync(request);
      }
      CompletableFuture<SyncResponse> future = new CompletableFuture<>();
      connectFutures.add(new CompletableFuture<Void>().whenComplete((connectResult, connectError) -> {
        if (connectError != null) {
          future.completeExceptionally(connectError);
        } else {
          client.sync(request).whenComplete((syncResponse, syncError) -> {
            if (syncError != null) {
              future.completeExceptionally(syncError);
            } else {
              future.complete(syncResponse);
            }
          });
        }
      }));
      return future;
    }

    @Override
    public CompletableFuture<PollResponse> poll(PollRequest request) {
      if (connected) {
        return client.poll(request);
      }
      CompletableFuture<PollResponse> future = new CompletableFuture<>();
      connectFutures.add(new CompletableFuture<Void>().whenComplete((connectResult, connectError) -> {
        if (connectError != null) {
          future.completeExceptionally(connectError);
        } else {
          client.poll(request).whenComplete((pollResponse, pollError) -> {
            if (pollError != null) {
              future.completeExceptionally(pollError);
            } else {
              future.complete(pollResponse);
            }
          });
        }
      }));
      return future;
    }

    @Override
    public CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
      if (connected) {
        return client.submit(request);
      }
      CompletableFuture<SubmitResponse> future = new CompletableFuture<>();
      connectFutures.add(new CompletableFuture<Void>().whenComplete((connectResult, connectError) -> {
        if (connectError != null) {
          future.completeExceptionally(connectError);
        } else {
          client.submit(request).whenComplete((submitResponse, submitError) -> {
            if (submitError != null) {
              future.completeExceptionally(submitError);
            } else {
              future.complete(submitResponse);
            }
          });
        }
      }));
      return future;
    }
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof RemoteNode && ((Node<?>) object).member().equals(member());
  }

  @Override
  public int hashCode() {
    int hashCode = 191;
    hashCode = 37 * hashCode + member().hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("RemoteNode[id=%s]", member().id());
  }

}
