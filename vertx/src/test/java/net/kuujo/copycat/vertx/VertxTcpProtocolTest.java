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
package net.kuujo.copycat.vertx;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolServer;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Netty TCP protocol test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class VertxTcpProtocolTest extends ConcurrentTestCase {

  /**
   * Tests sending from a client to sever and back.
   */
  public void testSendReceive() throws Throwable {
    VertxTcpProtocol protocol = new VertxTcpProtocol();
    ProtocolServer server = protocol.createServer(new URI("tcp://localhost:5555"));
    ProtocolClient client = protocol.createClient(new URI("tcp://localhost:5555"));

    server.handler(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      threadAssertEquals(new String(bytes), "Hello world!");
      return CompletableFuture.completedFuture(ByteBuffer.wrap("Hello world back!".getBytes()));
    });
    server.listen().thenRun(this::resume);
    await(5000);

    client.connect().thenRun(this::resume);
    await(5000);

    client.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAccept(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      threadAssertEquals(new String(bytes), "Hello world back!");
      resume();
    });
    await(5000);

    client.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAccept(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      threadAssertEquals(new String(bytes), "Hello world back!");
      resume();
    });
    await(5000);

    client.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAccept(buffer -> {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      threadAssertEquals(new String(bytes), "Hello world back!");
      resume();
    });
    await(5000);
  }

}
