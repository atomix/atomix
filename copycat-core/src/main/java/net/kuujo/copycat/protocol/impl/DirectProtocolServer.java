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
package net.kuujo.copycat.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.InstallRequest;
import net.kuujo.copycat.protocol.InstallResponse;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.PingResponse;
import net.kuujo.copycat.protocol.PollRequest;
import net.kuujo.copycat.protocol.PollResponse;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * Direct protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectProtocolServer implements ProtocolServer {
  private final String address;
  private final CopyCatContext context;
  private AsyncCallback<PingRequest> pingCallback;
  private AsyncCallback<SyncRequest> syncCallback;
  private AsyncCallback<InstallRequest> installCallback;
  private AsyncCallback<PollRequest> pollCallback;
  private AsyncCallback<SubmitRequest> submitCallback;

  public DirectProtocolServer(String address, CopyCatContext context) {
    this.address = address;
    this.context = context;
  }

  @Override
  public void pingCallback(AsyncCallback<PingRequest> callback) {
    pingCallback = callback;
  }

  void ping(PingRequest request, AsyncCallback<PingResponse> callback) {
    request.responseCallback(callback);
    if (pingCallback != null) {
      pingCallback.complete(request);
    }
  }

  @Override
  public void syncCallback(AsyncCallback<SyncRequest> callback) {
    syncCallback = callback;
  }

  void sync(SyncRequest request, AsyncCallback<SyncResponse> callback) {
    request.responseCallback(callback);
    if (syncCallback != null) {
      syncCallback.complete(request);
    }
  }

  @Override
  public void installCallback(AsyncCallback<InstallRequest> callback) {
    installCallback = callback;
  }

  void install(InstallRequest request, AsyncCallback<InstallResponse> callback) {
    request.responseCallback(callback);
    if (installCallback != null) {
      installCallback.complete(request);
    }
  }

  @Override
  public void pollCallback(AsyncCallback<PollRequest> callback) {
    pollCallback = callback;
  }

  void poll(PollRequest request, AsyncCallback<PollResponse> callback) {
    request.responseCallback(callback);
    if (pollCallback != null) {
      pollCallback.complete(request);
    }
  }

  @Override
  public void submitCallback(AsyncCallback<SubmitRequest> callback) {
    submitCallback = callback;
  }

  void submit(SubmitRequest request, AsyncCallback<SubmitResponse> callback) {
    request.responseCallback(callback);
    if (submitCallback != null) {
      submitCallback.complete(request);
    }
  }

  @Override
  public void start(AsyncCallback<Void> callback) {
    context.registry().bind(address, this);
    callback.complete(null);
  }

  @Override
  public void stop(AsyncCallback<Void> callback) {
    context.registry().unbind(address);
    callback.complete(null);
  }

}
