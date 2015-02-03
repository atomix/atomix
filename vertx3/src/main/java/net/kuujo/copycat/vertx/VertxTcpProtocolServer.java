/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.vertx;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import net.kuujo.copycat.protocol.ProtocolHandler;
import net.kuujo.copycat.protocol.ProtocolServer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Vert.x TCP protocol server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxTcpProtocolServer implements ProtocolServer {
  private Vertx vertx = Vertx.vertx();
  private final String host;
  private final int port;
  private final VertxTcpProtocol protocol;
  private NetServer server;
  private ProtocolHandler handler;

  public VertxTcpProtocolServer(String host, int port, VertxTcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  @Override
  public void handler(ProtocolHandler handler) {
    this.handler = handler;
  }

  @Override
  public CompletableFuture<Void> listen() {
    final CompletableFuture<Void> future = new CompletableFuture<>();

    if (server == null) {
      NetServerOptions options = new NetServerOptions()
        .setTcpKeepAlive(true)
        .setTcpNoDelay(true)
        .setReuseAddress(true)
        .setAcceptBacklog(protocol.getAcceptBacklog())
        .setSendBufferSize(protocol.getSendBufferSize())
        .setReceiveBufferSize(protocol.getReceiveBufferSize())
        .setSsl(protocol.isSsl())
        .setClientAuthRequired(protocol.isClientAuthRequired())
        .setUsePooledBuffers(true);
      server = vertx.createNetServer(options);
      server.connectHandler(socket -> {
        RecordParser parser = RecordParser.newFixed(4, null);
        Handler<Buffer> handler = new Handler<Buffer>() {
          int length = -1;
          @Override
          public void handle(Buffer buffer) {
            if (length == -1) {
              length = buffer.getInt(0);
              parser.fixedSizeMode(length + 8);
            } else {
              handleRequest(buffer.getLong(0), socket, buffer.getBuffer(8, length + 8).getByteBuf().nioBuffer());
              length = -1;
              parser.fixedSizeMode(4);
            }
          }
        };
        parser.setOutput(handler);
        socket.handler(parser);
      }).listen(port, host, result -> {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(null);
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  /**
   * Handles a request.
   */
  private void handleRequest(final long id, final NetSocket socket, final ByteBuffer request) {
    if (handler != null) {
      handler.apply(request).whenComplete((response, error) -> {
        if (error == null) {
          respond(socket, id, response);
        }
      });
    }
  }

  /**
   * Responds to a request from the given socket.
   */
  private void respond(NetSocket socket, long id, ByteBuffer response) {
    int length = response.remaining();
    byte[] bytes = new byte[length];
    response.get(bytes);
    socket.write(Buffer.buffer().appendInt(length).appendLong(id).appendBytes(bytes));
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (server != null) {
      server.close(result -> {
        if (result.failed()) {
          future.completeExceptionally(result.cause());
        } else {
          future.complete(null);
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  @Override
  public String toString() {
    return String.format("%s[host=%s, port=%d]", getClass().getSimpleName(), host, port);
  }

}
