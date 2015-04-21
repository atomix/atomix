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
package net.kuujo.copycat.io.util;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * Direct ByteBuffer based memory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectMemory extends NativeMemory {
  public static final long MAX_SIZE = Integer.MAX_VALUE - 5;

  /**
   * Allocates direct memory via {@link DirectMemoryAllocator}.
   *
   * @param size The size of the memory to allocate.
   * @return The allocated memory.
   */
  public static DirectMemory allocate(long size) {
    if (size > MAX_SIZE)
      throw new IllegalArgumentException("size cannot be greater than " + MAX_SIZE);
    return new DirectMemoryAllocator().allocate(size);
  }

  protected final ByteBuffer buffer;

  public DirectMemory(ByteBuffer buffer, MemoryAllocator<? extends NativeMemory> allocator) {
    super(((DirectBuffer) buffer).address(), buffer.capacity(), allocator);
    this.buffer = buffer;
  }

  @Override
  public void free() {
    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
    if (cleaner != null)
      cleaner.clean();
  }

}
