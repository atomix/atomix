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
package net.kuujo.copycat.state.impl;

import java.util.concurrent.CompletableFuture;

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

/**
 * Non-existent state.<p>
 *
 * The <code>None</code> state is used to represent a non-existent
 * state in a CopyCat context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class None extends RaftState {

  @Override
  public void init(RaftStateContext context) {
    // Don't call super.init() here so that server handlers won't be registered.
    context.setCurrentLeader(null);
  }

  @Override
  public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
    return CompletableFuture.completedFuture(new AppendEntriesResponse(request.id(), "Replica is not alive"));
  }

  @Override
  public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    return CompletableFuture.completedFuture(new RequestVoteResponse(request.id(), "Replica is not alive"));
  }

  @Override
  public CompletableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request) {
    return CompletableFuture.completedFuture(new SubmitCommandResponse(request.id(), "Replica is not alive"));
  }

  @Override
  public void destroy() {
    // Do nothing.
  }

}
