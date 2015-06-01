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
package net.kuujo.copycat.manager;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.raft.Operation;

/**
 * Base path operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class PathOperation<T> implements Operation<T>, Writable {
  protected String path;

  protected PathOperation() {
  }

  protected PathOperation(String path) {
    this.path = path;
  }

  /**
   * Returns the path.
   *
   * @return The path.
   */
  public String path() {
    return path;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    buffer.writeInt(path.getBytes().length).write(path.getBytes());
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    byte[] bytes = new byte[buffer.readInt()];
    buffer.read(bytes);
    path = new String(bytes);
  }

  /**
   * Path command builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends PathOperation<?>> extends Operation.Builder<U> {
    protected final U operation;

    protected Builder(U operation) {
      super(operation);
      this.operation = operation;
    }

    /**
     * Sets the command path.
     *
     * @param path The command path.
     * @return The command builder.
     */
    @SuppressWarnings("unchecked")
    public T withPath(String path) {
      operation.path = path;
      return (T) this;
    }
  }

}
