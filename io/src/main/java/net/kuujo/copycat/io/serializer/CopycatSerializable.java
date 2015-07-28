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
package net.kuujo.copycat.io.serializer;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;

/**
 * Provides an interface for serializable types.
 * <p>
 * Classes can implement this interface as an alternative to providing a separate {@link TypeSerializer} instance. Note,
 * however, that {@link CopycatSerializable} classes must still be registered via {@link Serializer#register(Class)}.
 * <p>
 * Types that implement this interface should provide a no-argument constructor via which Copycat can allocate new
 * instances of the class. During serialization, Copycat will call {@link CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, Serializer)}
 * to serialize the object to a {@link net.kuujo.copycat.io.Buffer}. During deserialization, Copycat will call
 * {@link CopycatSerializable#readObject(net.kuujo.copycat.io.BufferInput, Serializer)} to deserialize
 * the object from a {@link net.kuujo.copycat.io.Buffer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CopycatSerializable {

  /**
   * Writes the object to the given buffer.
   * <p>
   * Implementations of this method should write object attributes to the given buffer. Note that assumptions cannot be
   * safely made about the given buffer's {@link net.kuujo.copycat.io.Buffer#position()}, {@link net.kuujo.copycat.io.Buffer#limit()},
   * or any other navigational attributes of the provided {@link net.kuujo.copycat.io.Buffer}. To navigate the buffer,
   * set the buffer's {@link net.kuujo.copycat.io.Buffer#mark()} and {@link net.kuujo.copycat.io.Buffer#reset()}.
   * <p>
   * When writing dynamically sized attributes such as strings and collections, users should always write the attribute's
   * size to the given buffer. Copycat makes no guarantee that the buffer provided to
   * {@link CopycatSerializable#readObject(net.kuujo.copycat.io.BufferInput, Serializer)} will reflect the
   * number of bytes written to the buffer during serialization.
   *
   * @param buffer The buffer to which to write the object.
   * @param serializer The serializer with which the object is being serialized.
   * @see CopycatSerializable#readObject(net.kuujo.copycat.io.BufferInput, Serializer)
   */
  void writeObject(BufferOutput buffer, Serializer serializer);

  /**
   * Reads the object from the given buffer.
   * <p>
   * Implementations of this method should read object attributes from the given buffer in the same order with which they
   * were written to the buffer in {@link CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, Serializer)}.
   * Copycat guarantees only that the current {@link net.kuujo.copycat.io.Buffer#position()} will reflect the start
   * of the bytes written by {@link CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, Serializer)},
   * but not that the {@link net.kuujo.copycat.io.Buffer#remaining()} bytes reflect the number of bytes written by
   * {@link CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, Serializer)}.
   *
   * @param buffer The buffer from which to read the object.
   * @param serializer The serializer with which the object is being serialized.
   * @see CopycatSerializable#writeObject(net.kuujo.copycat.io.BufferOutput, Serializer)
   */
  void readObject(BufferInput buffer, Serializer serializer);

}
