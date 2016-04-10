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
package io.atomix.manager.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Operation;

/**
 * Base key operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class KeyOperation<T> implements Operation<T>, CatalystSerializable {
  protected String key;

  public KeyOperation() {
  }

  /**
   * @throws NullPointerException if {@code key} is null
   */
  public KeyOperation(String key) {
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

}
