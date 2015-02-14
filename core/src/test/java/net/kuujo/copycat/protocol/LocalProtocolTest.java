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
package net.kuujo.copycat.protocol;

import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Local protocol test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LocalProtocolTest extends ConcurrentTestCase {

  /**
   * Tests sending from a client to sever and back.
   */
  public void testSendReceive() throws Throwable {
    Protocol protocol = new LocalProtocol();
    ProtocolServer server = protocol.createServer(new URI("local://test"));
    ProtocolClient client = protocol.createClient(new URI("local://test"));

    server.connectListener(connection -> {
      connection.handler(buffer -> {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        threadAssertEquals(new String(bytes), "Hello world!");
        return CompletableFuture.completedFuture(ByteBuffer.wrap("Hello world back!".getBytes()));
      });
    });
    server.listen().thenRunAsync(this::resume);
    await(5000);

    expectResumes(3);
    client.connect().thenAccept(connection -> {
      connection.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });

      connection.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });

      connection.write(ByteBuffer.wrap("Hello world!".getBytes())).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });
    });
    await(5000);
  }

}
