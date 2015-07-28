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

import net.kuujo.copycat.Operation;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.io.serializer.Serializer;

/**
 * Resource operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ResourceOperation<T extends Operation<U>, U> implements Operation<U>, CopycatSerializable {
  protected long resource;
  protected T operation;

  /**
   * Returns the resource ID.
   *
   * @return The resource ID.
   */
  public long resource() {
    return resource;
  }

  /**
   * Returns the resource operation.
   *
   * @return The resource operation.
   */
  public T operation() {
    return operation;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(resource);
    serializer.writeObject(operation, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    resource = buffer.readLong();
    operation = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("ResourceOperation[resource=%s, operation=%s]", resource, operation.getClass().getSimpleName());
  }

}
