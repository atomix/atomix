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
package net.kuujo.copycat.io;

/**
 * Navigable buffer.
 * <p>
 * This interface provides the common methods for navigating a buffer. For understandability, buffer navigation methods
 * intentionally mimic those found in {@link java.nio.Buffer}. Users of the core Java {@code java.nio.Buffer} should
 * feel quite familiar with this interface and are encouraged to assume the same behaviors as those exposed by
 * {@code java.nio.Buffer}.
 * <p>
 * In order to support reading and writing from buffers, {@code NavigableBuffer} implementations maintain a series of
 * pointers to aid in navigating the buffer.
 * <p>
 * Most notable of these pointers is the {@code position}. When values are written to or read from the buffer, the
 * buffer increments its internal {@code position} according to the number of bytes read. This allows users to iterate
 * through the bytes in the buffer without maintaining external pointers.
 * <p>
 * <pre>
 *   {@code
 *      Buffer buffer = NativeBuffer.allocate(1024);
 *      buffer.writeInt(1);
 *      buffer.flip();
 *      assert buffer.readInt() == 1;
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface NavigableBytes<T extends NavigableBytes<?>> {

  /**
   * Returns the buffer's capacity.
   * <p>
   * The capacity represents the total amount of storage space allocated to the buffer by the underlying storage
   * implementation. As such, capacity is defined at the time of the object construction and cannot change.
   *
   * @return The buffer's capacity.
   */
  long capacity();

  /**
   * Returns the buffer's current read/write position.
   * <p>
   * The position is an internal cursor that tracks where to write/read bytes in the underlying storage implementation.
   *
   * @return The buffer's current position.
   */
  long position();

  /**
   * Sets the buffer's current read/write position.
   * <p>
   * The position is an internal cursor that tracks where to write/read bytes in the underlying storage implementation.
   *
   * @param position The position to set.
   * @return This buffer.
   * @throws java.lang.IllegalArgumentException If the given position is less than {@code 0} or more than {@link NavigableBytes#limit()}
   */
  T position(long position);

  /**
   * Returns tbe buffer's read/write limit.
   * <p>
   * The limit dictates the highest position to which bytes can be read from or written to the buffer. By default, the
   * limit is initialized to {@link NavigableBytes#capacity()}, but it may be explicitly set via
   * {@link NavigableBytes#limit(long)} or {@link NavigableBytes#flip()}.
   *
   * @return The buffer's limit.
   */
  long limit();

  /**
   * Sets the buffer's read/write limit.
   * <p>
   * The limit dictates the highest position to which bytes can be read from or written to the buffer. The limit must
   * be within the bounds of the buffer, i.e. greater than {@code 0} and less than or equal to {@link NavigableBytes#capacity()}
   *
   * @param limit The limit to set.
   * @return This buffer.
   * @throws java.lang.IllegalArgumentException If the given limit is less than {@code 0} or more than {@link NavigableBytes#capacity()}
   */
  T limit(long limit);

  /**
   * Returns the number of bytes remaining in the buffer until the {@link NavigableBytes#limit()} is reached.
   * <p>
   * The bytes remaining is calculated by {@code buffer.limit() - buffer.position()}
   *
   * @return The number of bytes remaining in the buffer.
   */
  long remaining();

  /**
   * Returns a boolean indicating whether the buffer has bytes remaining.
   * <p>
   * If {@link NavigableBytes#remaining()} is greater than {@code 0} then this method will return {@code true}, otherwise
   * {@code false}
   *
   * @return Indicates whether bytes are remaining in the buffer. {@code true} if {@link NavigableBytes#remaining()} is
   *         greater than {@code 0}, {@code false} otherwise.
   */
  boolean hasRemaining();

  /**
   * Flips the buffer. The limit is set to the current position and then the position is set to zero. If the mark is
   * defined then it is discarded.
   *
   * @return This buffer.
   */
  T flip();

  /**
   * Sets a mark at the current position.
   * <p>
   * The mark is a simple internal reference to the buffer's current position. Marks can be used to reset the buffer
   * to a specific position after some operation.
   * <p>
   * <pre>
   *   {@code
   *   buffer.mark();
   *   buffer.writeInt(1).writeBoolean(true);
   *   buffer.reset();
   *   assert buffer.readInt() == 1;
   *   }
   * </pre>
   *
   * @return This buffer.
   */
  T mark();

  /**
   * Resets the buffer's position to the previously-marked position.
   * <p>
   * Invoking this method neither changes nor discards the mark's value.
   *
   * @return This buffer.
   */
  T reset();

  /**
   * Rewinds the buffer. The position is set to zero and the mark is discarded.
   *
   * @return This buffer.
   */
  T rewind();

  /**
   * Clears the buffer. The position is set to zero, the limit is set to the capacity, and the mark is discarded.
   *
   * @return This buffer.
   */
  T clear();

}
