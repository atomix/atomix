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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;

import java.nio.ByteBuffer;

/**
 * Protocol reader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ProtocolReader {
  private final Kryo kryo;
  private final ByteBuffer buffer = ByteBuffer.allocate(4096);
  private final ByteBufferInput input = new ByteBufferInput(buffer);

  public ProtocolReader() {
    this.kryo = new Kryo();
    kryo.register(Requests.CONFIGURE.type(), Requests.CONFIGURE.id());
    kryo.register(Responses.CONFIGURE.type(), Responses.CONFIGURE.id());
    kryo.register(Requests.PING.type(), Requests.PING.id());
    kryo.register(Responses.PING.type(), Responses.PING.id());
    kryo.register(Requests.POLL.type(), Requests.POLL.id());
    kryo.register(Responses.POLL.type(), Responses.POLL.id());
    kryo.register(Requests.SYNC.type(), Requests.SYNC.id());
    kryo.register(Responses.SYNC.type(), Responses.SYNC.id());
    kryo.register(Requests.COMMIT.type(), Requests.COMMIT.id());
    kryo.register(Responses.COMMIT.type(), Responses.COMMIT.id());
  }

  /**
   * Reads a protocol request.
   *
   * @param bytes The serialized request.
   * @param <T> The request type.
   * @return The request object.
   */
  @SuppressWarnings("unchecked")
  public <T extends Request> T readRequest(byte[] bytes) {
    buffer.put(bytes);
    buffer.rewind();
    input.setBuffer(buffer);
    T request = (T) kryo.readClassAndObject(input);
    buffer.clear();
    return request;
  }

  /**
   * Reads a protocol response.
   *
   * @param bytes The serialized response.
   * @param <T> The response type.
   * @return The response object.
   */
  @SuppressWarnings("unchecked")
  public <T extends Response> T readResponse(byte[] bytes) {
    buffer.put(bytes);
    buffer.rewind();
    input.setBuffer(buffer);
    T response = (T) kryo.readClassAndObject(input);
    buffer.clear();
    return response;
  }

}
