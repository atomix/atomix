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

/**
 * Storage block.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Block extends Bytes<Block>, AutoCloseable {

  /**
   * Returns the index of the block in the parent storage.
   * <p>
   * Indexes are zero-based references to the position of the block within the parent storage implementation. For
   * instance, if the storage instance was initialized with a block size of {@code 1024} then block {@code 1} will be
   * bytes {@code 1024} through {@code 2047}.
   *
   * @return The index of the block.
   */
  int index();

  /**
   * Returns a block reader.
   * <p>
   * The block reader is a special buffer type designed for writing to storage blocks. The returned block will have
   * unique {@code position} and {@code limit} that are separate from other readers returned by this block. The block
   * will maintain a reference to the returned reader. Once bytes have been written to the block, close the reader
   * via {@link BlockReader#close()} to release the reader back to the block. This allows blocks to recycle existing
   * readers with {@link ReusableBlockReaderPool} rather than creating a new block for each new write.
   *
   * @return The block reader.
   */
  BlockReader reader();

  /**
   * Returns a block writer.
   * <p>
   * The block writer is a special buffer type designed for writing to storage blocks. The returned block will have
   * unique {@code position} and {@code limit} that are separate from other writers returned by this block. The block
   * will maintain a reference to the returned writer. Once bytes have been written to the block, close the writer
   * via {@link BlockWriter#close()} to release the writer back to the block. This allows blocks to recycle existing
   * writers with {@link ReusableBlockWriterPool} rather than creating a new block for each new write.
   *
   * @return The block writer.
   */
  BlockWriter writer();

}
