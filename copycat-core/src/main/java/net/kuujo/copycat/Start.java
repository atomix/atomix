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
package net.kuujo.copycat;

import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.InstallResponse;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * Start state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Start extends BaseState {

  @Override
  public void init(CopyCatContext context) {
    // Don't call super.init() here so that server handlers won't be registered.
    context.setCurrentLeader(null);
  }

  @Override
  public void ping(PingRequest request, AsyncCallback<PingResponse> responseCallback) {
    responseCallback.complete(new PingResponse("Replica is not alive"));
  }

  @Override
  public void sync(SyncRequest request, AsyncCallback<SyncResponse> responseCallback) {
    responseCallback.complete(new SyncResponse("Replica is not alive"));
  }

  @Override
  public void install(InstallRequest request, AsyncCallback<InstallResponse> responseCallback) {
    responseCallback.complete(new InstallResponse("Replica is not alive"));
  }

  @Override
  public void poll(PollRequest request, AsyncCallback<PollResponse> responseCallback) {
    responseCallback.complete(new PollResponse("Replica is not alive"));
  }

  @Override
  public void submit(SubmitRequest request, AsyncCallback<SubmitResponse> responseCallback) {
    responseCallback.complete(new SubmitResponse("Replica is not alive"));
  }

  @Override
  public void destroy() {
    // Do nothing.
  }

}
