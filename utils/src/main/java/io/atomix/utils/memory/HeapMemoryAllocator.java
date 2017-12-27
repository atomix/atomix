/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.utils.memory;

/**
 * Java heap memory allocator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HeapMemoryAllocator implements MemoryAllocator<HeapMemory> {

  @Override
  public HeapMemory allocate(int size) {
    if (size > HeapMemory.MAX_SIZE)
      throw new IllegalArgumentException("size cannot be greater than " + HeapMemory.MAX_SIZE);
    return new HeapMemory(new byte[(int) size], this);
  }

  @Override
  public HeapMemory reallocate(HeapMemory memory, int size) {
    HeapMemory copy = allocate(size);
    NativeMemory.UNSAFE.copyMemory(memory.array(), HeapMemory.ARRAY_BASE_OFFSET, copy.array(), HeapMemory.ARRAY_BASE_OFFSET, Math.min(size, memory.size()));
    memory.free();
    return copy;
  }

}
