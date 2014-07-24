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
import net.kuujo.copycat.protocol.ProtocolHandler;
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
  private ProtocolHandler requestHandler;

  public DirectProtocolServer(String address, CopyCatContext context) {
    this.address = address;
    this.context = context;
  }

  @Override
  public void protocolHandler(ProtocolHandler handler) {
    this.requestHandler = handler;
  }

  void ping(PingRequest request, AsyncCallback<PingResponse> callback) {
    if (requestHandler != null) {
      requestHandler.ping(request, callback);
    }
  }

  void sync(SyncRequest request, AsyncCallback<SyncResponse> callback) {
    if (requestHandler != null) {
      requestHandler.sync(request, callback);
    }
  }

  void install(InstallRequest request, AsyncCallback<InstallResponse> callback) {
    if (requestHandler != null) {
      requestHandler.install(request, callback);
    }
  }

  void poll(PollRequest request, AsyncCallback<PollResponse> callback) {
    if (requestHandler != null) {
      requestHandler.poll(request, callback);
    }
  }

  void submit(SubmitRequest request, AsyncCallback<SubmitResponse> callback) {
    if (requestHandler != null) {
      requestHandler.submit(request, callback);
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
