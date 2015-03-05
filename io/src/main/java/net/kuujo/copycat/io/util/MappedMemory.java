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

import java.nio.MappedByteBuffer;

/**
 * Mapped memory.
 * <p>
 * This is a special memory descriptor that handles management of {@link java.nio.MappedByteBuffer} based memory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MappedMemory implements Memory {
  private final MappedByteBuffer buffer;

  public MappedMemory(MappedByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public long address() {
    return ((DirectBuffer) buffer).address();
  }

  @Override
  public long size() {
    return buffer.capacity();
  }

  @Override
  public void clear() {
    buffer.clear();
  }

  @Override
  public void free() {
    Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
    if (cleaner != null) {
      cleaner.clean();
    }
  }

}
