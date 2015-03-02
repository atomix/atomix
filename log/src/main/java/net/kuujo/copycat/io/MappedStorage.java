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

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Mapped byte storage.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MappedStorage implements Storage, ReferenceManager<NativeBlock> {
  private static final int MAX_BLOCKS = 2;
  private final FileChannel channel;
  private final Map<Integer, NativeBlock> blocks = new HashMap<>(1024);
  private final long blockSize;

  public MappedStorage(File file, long blockSize) throws IOException {
    if (blockSize <= 0)
      throw new IllegalArgumentException("block size must be positive");
    this.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    this.blockSize = blockSize;
  }

  @Override
  public Block acquire(int index) {
    NativeBlock block = blocks.get(index);
    if (block == null) {
      synchronized (blocks) {
        block = blocks.get(index);
        if (block == null) {
          try {
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, blockSize * index, blockSize);
            block = new NativeBlock(index, new MappedMemory(buffer), this);
            blocks.put(index, block);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
    return block.acquire();
  }

  @Override
  public void release(NativeBlock reference) {
    NativeBlock block  = blocks.get(reference.index());
    if (block != null) {
      if (block.references() == 0 && blocks.size() > MAX_BLOCKS) {
        synchronized (blocks) {
          if (block.references() == 0) {
            block = blocks.remove(reference.index());
            if (block != null) {
              block.memory().free();
            }
          }
        }
      }
    }
  }

  @Override
  public void close() {
    synchronized (blocks) {
      blocks.values().forEach(b -> b.memory().free());
    }
  }

}
