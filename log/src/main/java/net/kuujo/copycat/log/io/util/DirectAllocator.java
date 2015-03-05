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
package net.kuujo.copycat.log.io.util;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Direct memory allocator.
 * <p>
 * This allocator implementation allocates memory via {@link sun.nio.ch.DirectBuffer}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectAllocator implements Allocator {
  private final Map<Long, DirectBuffer> buffers = new HashMap<>(1024);

  @Override
  public Memory allocate(long size) {
    if (size > Integer.MAX_VALUE)
      throw new IllegalArgumentException("cannot allocate direct memory larger than 2GB");
    DirectBuffer buffer = (DirectBuffer) ByteBuffer.allocateDirect((int) size);
    buffers.put(buffer.address(), buffer);
    return new NativeMemory(buffer.address(), size, this);
  }

  @Override
  public void free(Memory memory) {
    DirectBuffer buffer = buffers.remove(memory.address());
    if (buffer != null) {
      Cleaner cleaner = buffer.cleaner();
      if (cleaner != null) {
        cleaner.clean();
      }
    }
  }

}
