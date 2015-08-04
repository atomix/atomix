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

/**
 * Direct memory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectMemory extends NativeMemory {

  /**
   * Allocates direct memory via {@link net.kuujo.copycat.io.util.DirectMemoryAllocator}.
   *
   * @param size The count of the memory to allocate.
   * @return The allocated memory.
   */
  public static DirectMemory allocate(long size) {
    return new DirectMemoryAllocator().allocate(size);
  }

  public DirectMemory(long address, long size, DirectMemoryAllocator allocator) {
    super(address, size, allocator);
  }

}
