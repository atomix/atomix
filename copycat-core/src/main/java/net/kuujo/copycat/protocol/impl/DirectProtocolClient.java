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
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.SubmitRequest;
import net.kuujo.copycat.protocol.SubmitResponse;
import net.kuujo.copycat.protocol.SyncRequest;
import net.kuujo.copycat.protocol.SyncResponse;
import net.kuujo.copycat.util.AsyncCallback;

/**
 * Direct protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectProtocolClient implements ProtocolClient {
  private final String address;
  private final CopyCatContext context;

  public DirectProtocolClient(String address, CopyCatContext context) {
    this.address = address;
    this.context = context;
  }

  @Override
  public void ping(PingRequest request, AsyncCallback<PingResponse> callback) {
    DirectProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.ping(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

  @Override
  public void sync(SyncRequest request, AsyncCallback<SyncResponse> callback) {
    DirectProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.sync(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

  @Override
  public void install(InstallRequest request, AsyncCallback<InstallResponse> callback) {
    DirectProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.install(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

  @Override
  public void poll(PollRequest request, AsyncCallback<PollResponse> callback) {
    DirectProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.poll(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

  @Override
  public void submit(SubmitRequest request, AsyncCallback<SubmitResponse> callback) {
    DirectProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.submit(request, callback);
    } else {
      callback.fail(new ProtocolException("Invalid server address"));
    }
  }

}
