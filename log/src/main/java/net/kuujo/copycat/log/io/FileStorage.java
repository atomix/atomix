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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * File storage.
 * <p>
 * This is a {@link java.io.RandomAccessFile} based storage implementation. This storage implementation is intended to
 * be used for strongly consistent, critical logging. Blocks and buffers provided by the same {@code FileStorage}
 * instance are all backed by the same {@code RandomAccessFile} instance. Writes and reads to and from {@code FileStorage}
 * provided blocks and buffers are not buffered and will immediately hit disk.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileStorage implements Storage {
  private final int blockSize;
  private final RandomAccessFile file;

  public FileStorage(File file, int blockSize) throws IOException {
    if (blockSize <= 0)
      throw new IllegalArgumentException("block size must be positive");
    this.blockSize = blockSize;
    this.file = new RandomAccessFile(file, "rw");
  }

  @Override
  public Block acquire(int index) {
    return new Block(index, new FileBytes(file, index * blockSize, blockSize));
  }

  @Override
  public void close() throws IOException {
    file.close();
  }

}
