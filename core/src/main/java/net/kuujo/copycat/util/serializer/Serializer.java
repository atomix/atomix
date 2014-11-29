/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.util.serializer;

import net.kuujo.copycat.util.serializer.internal.KryoSerializer;

/**
 * Serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Serializer {

  /**
   * Returns a serializer instance.
   *
   * @return A serializer instance.
   */
  static Serializer serializer() {
    return new KryoSerializer();
  }

  /**
   * Reads an object.
   *
   * @param bytes The object bytes.
   * @param <T> The object type.
   * @return The object.
   */
  <T> T readObject(byte[] bytes);

  /**
   * Writes an object.
   *
   * @param object The object to write.
   * @param <T> The object type.
   * @return The object bytes.
   */
  <T> byte[] writeObject(T object);

}
