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
 * Direct memory allocator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectMemoryAllocator implements MemoryAllocator<NativeMemory> {

  @Override
  public DirectMemory allocate(int size) {
    DirectMemory memory = new DirectMemory(DirectMemory.UNSAFE.allocateMemory(size), size, this);
    DirectMemory.UNSAFE.setMemory(memory.address(), size, (byte) 0);
    return memory;
  }

  @Override
  public DirectMemory reallocate(NativeMemory memory, int size) {
    DirectMemory newMemory = new DirectMemory(DirectMemory.UNSAFE.reallocateMemory(memory.address(), size), size, this);
    if (newMemory.size() > memory.size()) {
      DirectMemory.UNSAFE.setMemory(newMemory.address(), newMemory.size() - memory.size(), (byte) 0);
    }
    return newMemory;
  }

}
