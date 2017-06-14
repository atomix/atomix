/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.server.storage.util;

import io.atomix.util.buffer.BufferInput;
import io.atomix.util.buffer.BufferOutput;

/**
 * Storage type serializer.
 */
public interface StorageSerializer<T> {

  /**
   * Writes the object to the given output.
   *
   * @param output The output to which to write the object.
   * @param object The object to write.
   */
  void writeObject(BufferOutput output, T object);

  /**
   * Reads the object from the given input.
   *
   * @param input The input from which to read the object.
   * @param type The object type.
   * @return The object.
   */
  T readObject(BufferInput input, Class<T> type);

}
