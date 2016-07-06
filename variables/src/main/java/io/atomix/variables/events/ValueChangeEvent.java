/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.variables.events;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

/**
 * Value change event.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ValueChangeEvent<T> implements CatalystSerializable {
  private T oldValue;
  private T newValue;

  public ValueChangeEvent() {
  }

  public ValueChangeEvent(T oldValue, T newValue) {
    this.oldValue = oldValue;
    this.newValue = newValue;
  }

  /**
   * Returns the old value.
   *
   * @return The old value.
   */
  public T oldValue() {
    return oldValue;
  }

  /**
   * Returns the new value.
   *
   * @return The new value.
   */
  public T newValue() {
    return newValue;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    serializer.writeObject(oldValue, buffer);
    serializer.writeObject(newValue, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    oldValue = serializer.readObject(buffer);
    newValue = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[oldValue=%s, newValue=%s]", getClass().getSimpleName(), oldValue, newValue);
  }

}
