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
package net.kuujo.copycat.resource;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;

/**
 * Resource message.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceMessage<T> implements AlleycatSerializable {
  private long resource;
  private T message;

  public ResourceMessage() {
  }

  public ResourceMessage(long resource, T message) {
    this.resource = resource;
    this.message = message;
  }

  /**
   * Returns the resource ID.
   *
   * @return The resource ID.
   */
  public long resource() {
    return resource;
  }

  /**
   * Returns the message body.
   *
   * @return The message body.
   */
  public T message() {
    return message;
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat serializer) {
    buffer.writeLong(resource);
    serializer.writeObject(message, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat serializer) {
    resource = buffer.readLong();
    message = serializer.readObject(buffer);
  }

}
