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

import net.kuujo.copycat.io.util.ReferenceManager;
import net.kuujo.copycat.io.util.Referenceable;

/**
 * Block writer that delegates reference counting to the underlying buffer.
 * <p>
 * This writer is designed to handle reference counting for an underlying buffer. Instead of constructing a new writer
 * each time a block writer is opened, {@link ReusableBlockWriterPool} uses this class to determine when the use of a
 * writer has finished and recycle the writer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReusableBlockWriter<T extends Block & Referenceable<T>> extends BlockWriter {
  private final T buffer;
  private final ReferenceManager<ReusableBlockWriter<T>> referenceManager;

  public ReusableBlockWriter(T buffer, ReferenceManager<ReusableBlockWriter<T>> referenceManager) {
    super(buffer);
    if (buffer == null)
      throw new NullPointerException("buffer cannot be null");
    if (referenceManager == null)
      throw new NullPointerException("reference manager cannot be null");
    this.buffer = buffer;
    this.referenceManager = referenceManager;
    buffer.acquire();
  }

  T buffer() {
    return buffer;
  }

  @Override
  public void close() {
    referenceManager.release(this);
  }

}
