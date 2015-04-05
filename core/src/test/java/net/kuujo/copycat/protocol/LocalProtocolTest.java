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
import net.kuujo.copycat.io.HeapBuffer;
import org.testng.annotations.Test;

import java.net.URI;
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
        byte[] bytes = new byte[(int) buffer.remaining()];
        buffer.read(bytes);
        threadAssertEquals(new String(bytes), "Hello world!");
        return CompletableFuture.completedFuture(HeapBuffer.allocate("Hello world back!".getBytes().length).write("Hello world back!".getBytes()).flip());
      });
    });
    server.listen().thenRunAsync(this::resume);
    await(5000);

    expectResumes(3);
    client.connect().thenAccept(connection -> {
      connection.write(HeapBuffer.allocate("Hello world back!".getBytes().length).write("Hello world!".getBytes()).flip()).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[(int) buffer.remaining()];
        buffer.read(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });

      connection.write(HeapBuffer.allocate("Hello world back!".getBytes().length).write("Hello world!".getBytes()).flip()).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[(int) buffer.remaining()];
        buffer.read(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });

      connection.write(HeapBuffer.allocate("Hello world back!".getBytes().length).write("Hello world!".getBytes()).flip()).thenAcceptAsync(buffer -> {
        byte[] bytes = new byte[(int) buffer.remaining()];
        buffer.read(bytes);
        threadAssertEquals(new String(bytes), "Hello world back!");
        resume();
      });
    });
    await(5000);
  }

}
