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
package net.kuujo.copycat.log.io;

import net.kuujo.copycat.log.io.util.Allocator;

import java.util.HashMap;
import java.util.Map;

/**
 * Native memory storage.
 * <p>
 * This storage implementation allocates off-heap memory and provides direct access via {@link sun.misc.Unsafe}. It's
 * important to note that once a block has been acquired for the first time via {@link NativeStorage#acquire(int)}, the
 * memory allocated for that block will remain available for the lifetime of the storage instance (until
 * {@link NativeStorage#close()} is called).
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NativeStorage implements Storage {
  private final Allocator allocator;
  private final long blockSize;
  private final Map<Integer, NativeBytes> bytes = new HashMap<>(1024);

  public NativeStorage(Allocator allocator, long blockSize) {
    super();
    if (allocator == null)
      throw new NullPointerException("memory allocator cannot be null");
    if (blockSize <= 0)
      throw new IllegalArgumentException("block size must be positive");
    this.allocator = allocator;
    this.blockSize = blockSize;
  }

  @Override
  public Block acquire(int index) {
    return new Block(index, bytes.computeIfAbsent(index, i -> new NativeBytes(allocator.allocate(index * blockSize))));
  }

  @Override
  public void close() {
    bytes.values().forEach(b -> b.memory().free());
  }

}
