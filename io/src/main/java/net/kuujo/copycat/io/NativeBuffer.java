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

import net.kuujo.copycat.io.util.Allocator;
import net.kuujo.copycat.io.util.NativeAllocator;

/**
 * Native byte buffer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeBuffer extends CheckedBuffer {
  private static final Allocator allocator = new NativeAllocator();

  /**
   * Allocates a new native buffer.
   * <p>
   * When the buffer is constructed, {@link net.kuujo.copycat.io.util.NativeAllocator} will be used to allocate
   * {@code size} bytes of off-heap memory. Memory is accessed by the buffer directly via {@link sun.misc.Unsafe}.
   *
   * @param size The size of the buffer to allocate (in bytes).
   * @return The native buffer.
   */
  public static Buffer allocate(long size) {
    return new NativeBuffer(new NativeBytes(allocator.allocate(size)));
  }

  private final NativeBytes bytes;

  private NativeBuffer(NativeBytes bytes) {
    super(bytes);
    this.bytes = bytes;
  }

  @Override
  public void close() throws Exception {
    bytes.memory().free();
  }

}
