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

import net.kuujo.copycat.AsyncCallback;
import net.kuujo.copycat.AsyncResult;
import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

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

  void sync(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> callback) {
    if (requestHandler != null) {
      requestHandler.appendEntries(request, callback);
    }
  }

  void poll(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> callback) {
    if (requestHandler != null) {
      requestHandler.requestVote(request, callback);
    }
  }

  void submit(SubmitCommandRequest request, AsyncCallback<SubmitCommandResponse> callback) {
    if (requestHandler != null) {
      requestHandler.submitCommand(request, callback);
    }
  }

  @Override
  public void start(AsyncCallback<Void> callback) {
    context.registry().bind(address, this);
    callback.call(new AsyncResult<Void>((Void) null));
  }

  @Override
  public void stop(AsyncCallback<Void> callback) {
    context.registry().unbind(address);
    callback.call(new AsyncResult<Void>((Void) null));
  }

}
