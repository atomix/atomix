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

/**
 * Memory allocator.<p>
 *
 * Memory allocators handle allocation of off-heap memory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Allocator {

  /**
   * Allocates memory.
   *
   * @param size The size of the memory to allocate.
   * @return The allocated memory.
   */
  Memory allocate(long size);

  /**
   * Frees memory.
   * <p>
   * The memory will be freed using the provided {@link Memory#address()}.
   *
   * @param memory The memory to free.
   */
  void free(Memory memory);

}
