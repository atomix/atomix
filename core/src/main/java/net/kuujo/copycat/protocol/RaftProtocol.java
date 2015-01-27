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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.cluster.MessageHandler;
import net.kuujo.copycat.protocol.rpc.*;

import java.util.concurrent.CompletableFuture;

/**
 * Raft protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftProtocol extends Managed<Void> {

  /**
   * Sends a protocol sync request.
   *
   * @param request The protocol sync request.
   * @return A completable future to be completed with the sync response.
   */
  CompletableFuture<SyncResponse> sync(SyncRequest request);

  /**
   * Registers a protocol sync request handler.
   *
   * @param handler A protocol sync request handler.
   * @return The Raft protocol.
   */
  RaftProtocol syncHandler(MessageHandler<SyncRequest, SyncResponse> handler);

  /**
   * Sends a protocol ping request.
   *
   * @param request The protocol ping request.
   * @return A completable future to be completed with the ping response.
   */
  CompletableFuture<PingResponse> ping(PingRequest request);

  /**
   * Registers a protocol ping request handler.
   *
   * @param handler A protocol ping request handler.
   * @return The Raft protocol.
   */
  RaftProtocol pingHandler(MessageHandler<PingRequest, PingResponse> handler);

  /**
   * Sends a protocol poll request.
   *
   * @param request The protocol poll request.
   * @return A completable future to be completed with the poll response.
   */
  CompletableFuture<PollResponse> poll(PollRequest request);

  /**
   * Registers a protocol poll request handler.
   *
   * @param handler A protocol poll request handler.
   * @return The Raft protocol.
   */
  RaftProtocol pollHandler(MessageHandler<PollRequest, PollResponse> handler);

  /**
   * Sends a protocol append request.
   *
   * @param request The protocol append request.
   * @return A completable future to be completed with the append response.
   */
  CompletableFuture<AppendResponse> append(AppendRequest request);

  /**
   * Registers a protocol append request handler.
   *
   * @param handler A protocol append request handler.
   * @return The Raft protocol.
   */
  RaftProtocol appendHandler(MessageHandler<AppendRequest, AppendResponse> handler);

  /**
   * Sends a protocol query request.
   *
   * @param request The protocol query request.
   * @return A completable future to be completed with the query response.
   */
  CompletableFuture<QueryResponse> query(QueryRequest request);

  /**
   * Registers a protocol query request handler.
   *
   * @param handler A protocol query request handler.
   * @return The Raft protocol.
   */
  RaftProtocol queryHandler(MessageHandler<QueryRequest, QueryResponse> handler);

  /**
   * Sends a protocol commit request.
   *
   * @param request The protocol commit request.
   * @return A completable future to be completed with the commit response.
   */
  CompletableFuture<CommitResponse> commit(CommitRequest request);

  /**
   * Registers a protocol commit request handler.
   *
   * @param handler A protocol commit request handler.
   * @return The Raft protocol.
   */
  RaftProtocol commitHandler(MessageHandler<CommitRequest, CommitResponse> handler);

}
