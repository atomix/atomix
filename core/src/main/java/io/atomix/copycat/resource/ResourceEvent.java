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
package io.atomix.copycat.resource;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

/**
 * Resource event.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceEvent<T> implements CatalystSerializable {
  private long resource;
  private T message;

  public ResourceEvent() {
  }

  /**
   * @throws NullPointerException if {@code message} is null
   */
  public ResourceEvent(long resource, T message) {
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
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(resource);
    serializer.writeObject(message, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    resource = buffer.readLong();
    message = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[resource=%d, message=%s]", getClass().getSimpleName(), resource, message);
  }

}
