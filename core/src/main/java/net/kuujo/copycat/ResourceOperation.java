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
package net.kuujo.copycat;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.io.Buffer;
import net.kuujo.copycat.raft.Operation;

/**
 * Resource operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ResourceOperation<T extends Operation<U>, U> implements Operation<U>, AlleycatSerializable {
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
  public void writeObject(Buffer buffer, Alleycat alleycat) {
    buffer.writeLong(resource);
    alleycat.writeObject(operation, buffer);
  }

  @Override
  public void readObject(Buffer buffer, Alleycat alleycat) {
    resource = buffer.readLong();
    operation = alleycat.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("ResourceOperation[resource=%s, operation=%s]", resource, operation.getClass().getSimpleName());
  }
}
