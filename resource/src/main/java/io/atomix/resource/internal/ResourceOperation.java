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
 * limitations under the License
 */
package io.atomix.resource.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Operation;

/**
 * Base class for resource operations.
 *
 * @see ResourceCommand
 * @see ResourceQuery
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ResourceOperation<T extends Operation<U>, U> implements Operation<U>, CatalystSerializable {
  protected T operation;

  protected ResourceOperation() {
  }

  protected ResourceOperation(T operation) {
    this.operation = operation;
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
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    serializer.writeObject(operation, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    operation = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[operation=%s]", getClass().getSimpleName(), operation.getClass().getSimpleName());
  }

}
