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

import net.kuujo.copycat.Managed;
import net.kuujo.copycat.cluster.MessageHandler;

import java.util.concurrent.CompletableFuture;

/**
 * Raft protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RaftProtocol extends Managed {

  /**
   * Sends a protocol configure request.
   *
   * @param request The protocol configure request.
   * @return A completable future to be completed with the configure response.
   */
  CompletableFuture<ConfigureResponse> configure(ConfigureRequest request);

  /**
   * Registers a protocol configure request handler.
   *
   * @param handler A protocol configure request handler.
   * @return The Raft protocol.
   */
  RaftProtocol configureHandler(MessageHandler<ConfigureRequest, ConfigureResponse> handler);

  CompletableFuture<PingResponse> ping(PingRequest request);

  RaftProtocol pingHandler(MessageHandler<PingRequest, PingResponse> handler);

  CompletableFuture<PollResponse> poll(PollRequest request);

  RaftProtocol pollHandler(MessageHandler<PollRequest, PollResponse> handler);

  CompletableFuture<SyncResponse> sync(SyncRequest request);

  RaftProtocol syncHandler(MessageHandler<SyncRequest, SyncResponse> handler);

  CompletableFuture<CommitResponse> commit(CommitRequest request);

  RaftProtocol commitHandler(MessageHandler<CommitRequest, CommitResponse> handler);

}
