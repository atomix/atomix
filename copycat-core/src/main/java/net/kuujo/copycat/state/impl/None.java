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

import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.state.State;

/**
 * Non-existent state.<p>
 *
 * The <code>None</code> state is used to represent a non-existent
 * state in a CopyCat context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class None extends CopycatState {

  @Override
  public State.Type type() {
    return State.Type.NONE;
  }

  @Override
  public void init(CopycatStateContext context) {
    // Don't call super.init() here so that server handlers won't be registered.
    context.setCurrentLeader(null);
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return CompletableFuture.completedFuture(new SyncResponse(request.id(), "Replica is not alive"));
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return CompletableFuture.completedFuture(new PollResponse(request.id(), "Replica is not alive"));
  }

  @Override
  public CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
    return CompletableFuture.completedFuture(new SubmitResponse(request.id(), "Replica is not alive"));
  }

  @Override
  public void destroy() {
    // Do nothing.
  }

}
