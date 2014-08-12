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
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

/**
 * Local protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolClient implements ProtocolClient {
  private final String address;
  private final CopyCatContext context;

  public LocalProtocolClient(String address, CopyCatContext context) {
    this.address = address;
    this.context = context;
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncCallback<AppendEntriesResponse> callback) {
    LocalProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.sync(request, callback);
    } else {
      callback.call(new AsyncResult<AppendEntriesResponse>(new ProtocolException("Invalid server address")));
    }
  }

  @Override
  public void requestVote(RequestVoteRequest request, AsyncCallback<RequestVoteResponse> callback) {
    LocalProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.poll(request, callback);
    } else {
      callback.call(new AsyncResult<RequestVoteResponse>(new ProtocolException("Invalid server address")));
    }
  }

  @Override
  public void submitCommand(SubmitCommandRequest request, AsyncCallback<SubmitCommandResponse> callback) {
    LocalProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.submit(request, callback);
    } else {
      callback.call(new AsyncResult<SubmitCommandResponse>(new ProtocolException("Invalid server address")));
    }
  }

  @Override
  public void connect() {
  }

  @Override
  public void connect(AsyncCallback<Void> callback) {
    callback.call(new AsyncResult<Void>((Void) null));
  }

  @Override
  public void close() {
  }

  @Override
  public void close(AsyncCallback<Void> callback) {
    callback.call(new AsyncResult<Void>((Void) null));
  }

}
