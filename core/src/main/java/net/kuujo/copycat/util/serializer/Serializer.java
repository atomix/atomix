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

import java.nio.ByteBuffer;

/**
 * Serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Serializer<T> {

  /**
   * Returns the serialized type.
   *
   * @return The type serialized by this serializer.
   */
  Class<T> type();

  /**
   * Serializes the given object to a {@link ByteBuffer}
   *
   * @param object The object to serialize.
   * @param buffer The buffer to which to write the object.
   */
  void serialize(T object, ByteBuffer buffer);

}
