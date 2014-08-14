/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Request handler for requests between CopyCat replicas.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ProtocolHandler {

  /**
   * Sends or handles a protocol sync request.
   *
   * @param request The sync request.
   * @return A sync response future.
   */
  CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request);

  /**
   * Sends or handles a protocol poll request.
   *
   * @param request The poll request.
   * @return A poll response future.
   */
  CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);

  /**
   * Sends or handles a protocol submit request.
   *
   * @param request The submit request.
   * @return A submit response future.
   */
  CompletableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request);

}
