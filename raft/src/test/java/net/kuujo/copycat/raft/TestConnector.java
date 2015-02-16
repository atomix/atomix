/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.raft;

import net.kuujo.copycat.raft.protocol.*;
import net.kuujo.copycat.util.concurrent.Futures;

import java.net.ProtocolException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestConnector {
  private final Map<String, RaftContext> contexts = new HashMap<>();
  private final Map<String, Set<String>> connections = new HashMap<>();

  public TestConnector connect(RaftContext from, RaftContext to) {
    contexts.put(from.getConfig().getId(), from);
    contexts.put(to.getConfig().getId(), to);
    connections.computeIfAbsent(from.getConfig().getId(), id -> new HashSet<>()).add(to.getConfig().getId());
    from.joinHandler(request -> handleRequest(from.getConfig().getId(), request));
    from.promoteHandler(request -> handleRequest(from.getConfig().getId(), request));
    from.leaveHandler(request -> handleRequest(from.getConfig().getId(), request));
    from.syncHandler(request -> handleRequest(from.getConfig().getId(), request));
    from.pollHandler(request -> handleRequest(from.getConfig().getId(), request));
    from.voteHandler(request -> handleRequest(from.getConfig().getId(), request));
    from.appendHandler(request -> handleRequest(from.getConfig().getId(), request));
    from.queryHandler(request -> handleRequest(from.getConfig().getId(), request));
    from.commandHandler(request -> handleRequest(from.getConfig().getId(), request));
    return this;
  }

  public TestConnector disconnect(RaftContext from, RaftContext to) {
    connections.computeIfAbsent(from.getConfig().getId(), id -> new HashSet<>()).remove(to.getConfig().getId());
    return this;
  }

  @SuppressWarnings("unchecked")
  private <T extends Request, U extends Response> CompletableFuture<U> handleRequest(String id, T request) {
    Set<String> connections = this.connections.get(id);
    RaftContext context = contexts.get(request.id());
    if (connections != null && context != null && connections.contains(request.id())) {
      if (request instanceof JoinRequest) {
        return (CompletableFuture<U>) context.join((JoinRequest) request);
      } else if (request instanceof PromoteRequest) {
        return (CompletableFuture<U>) context.promote((PromoteRequest) request);
      } else if (request instanceof LeaveRequest) {
        return (CompletableFuture<U>) context.leave((LeaveRequest) request);
      } else if (request instanceof SyncRequest) {
        return (CompletableFuture<U>) context.sync((SyncRequest) request);
      } else if (request instanceof PollRequest) {
        return (CompletableFuture<U>) context.poll((PollRequest) request);
      } else if (request instanceof VoteRequest) {
        return (CompletableFuture<U>) context.vote((VoteRequest) request);
      } else if (request instanceof AppendRequest) {
        return (CompletableFuture<U>) context.append((AppendRequest) request);
      } else if (request instanceof QueryRequest) {
        return (CompletableFuture<U>) context.query((QueryRequest) request);
      } else if (request instanceof CommandRequest) {
        return (CompletableFuture<U>) context.command((CommandRequest) request);
      }
    }
    return Futures.exceptionalFuture(new ProtocolException("No member found"));
  }

}
