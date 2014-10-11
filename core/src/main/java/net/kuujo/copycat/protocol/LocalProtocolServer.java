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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.spi.protocol.ProtocolServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Synchronous local protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolServer implements ProtocolServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalProtocolServer.class);
  private final String id;
  private final Map<String, LocalProtocolServer> registry;
  private RequestHandler requestHandler;

  public LocalProtocolServer(String id, Map<String, LocalProtocolServer> registry) {
    this.id = id;
    this.registry = registry;
  }

  @Override
  public void requestHandler(RequestHandler handler) {
    this.requestHandler = handler;
  }

  PingResponse ping(PingRequest request) {
    Assert.isNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.ping(request);
  }

  SyncResponse sync(SyncRequest request) {
    Assert.isNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.sync(request);
  }

  PollResponse poll(PollRequest request) {
    Assert.isNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.poll(request);
  }

  SubmitResponse submit(SubmitRequest request) {
    Assert.isNotNull(requestHandler, "No protocol handler provided");
    return requestHandler.submit(request);
  }

  @Override
  public void listen() {
    LOGGER.debug("{} listening at {}", this, id);
    registry.put(id, this);
  }

  @Override
  public void close() {
    LOGGER.debug("{} closing server at {}", this, id);
    registry.remove(id);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), id);
  }

}
