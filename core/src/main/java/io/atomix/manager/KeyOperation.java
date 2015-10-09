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
package io.atomix.manager;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.copycat.client.Operation;

/**
 * Base key operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class KeyOperation<T> implements Operation<T>, CatalystSerializable {
  protected String key;

  protected KeyOperation() {
  }

  /**
   * @throws NullPointerException if {@code key} is null
   */
  protected KeyOperation(String key) {
    this.key = Assert.notNull(key, "key");
  }

  /**
   * Returns the key.
   *
   * @return The key.
   */
  public String key() {
    return key;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeUTF8(key);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    key = buffer.readUTF8();
  }

  /**
   * Key command builder.
   */
  public static abstract class Builder<T extends Builder<T, U, V>, U extends KeyOperation<V>, V> extends Operation.Builder<T, U, V> {

    protected Builder(BuilderPool<T, U> pool) {
      super(pool);
    }

    /**
     * Sets the command key.
     *
     * @param key The command key.
     * @return The command builder.
     * @throws NullPointerException if {@code key} is null
     */
    @SuppressWarnings("unchecked")
    public T withKey(String key) {
      operation.key = Assert.notNull(key, "key");
      return (T) this;
    }
  }

}
