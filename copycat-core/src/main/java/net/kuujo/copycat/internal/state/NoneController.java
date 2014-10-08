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
package net.kuujo.copycat.internal.state;

import net.kuujo.copycat.CopycatState;
import net.kuujo.copycat.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Non-existent state.<p>
 *
 * The <code>None</code> state is used to represent a non-existent
 * state in a CopyCat context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NoneController extends StateController {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoneController.class);

  @Override
  CopycatState state() {
    return CopycatState.NONE;
  }

  @Override
  Logger logger() {
    return LOGGER;
  }

  @Override
  public void init(StateContext context) {
    // Don't call super.init() here so that server handlers won't be registered.
    context.currentLeader(null);
  }

  @Override
  public CompletableFuture<PingResponse> ping(PingRequest request) {
    return CompletableFuture.completedFuture(logResponse(new PingResponse(logRequest(request).id(), "Replica is not alive")));
  }

  @Override
  public CompletableFuture<SyncResponse> sync(SyncRequest request) {
    return CompletableFuture.completedFuture(logResponse(new SyncResponse(logRequest(request).id(), "Replica is not alive")));
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return CompletableFuture.completedFuture(logResponse(new PollResponse(logRequest(request).id(), "Replica is not alive")));
  }

  @Override
  public CompletableFuture<SubmitResponse> submit(SubmitRequest request) {
    return CompletableFuture.completedFuture(logResponse(new SubmitResponse(logRequest(request).id(), "Replica is not alive")));
  }

  @Override
  void destroy() {
    // Do nothing.
  }

  @Override
  public String toString() {
    return "None[context=null]";
  }

}
