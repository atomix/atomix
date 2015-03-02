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
 * Buffer reader that delegates reference counting to the underlying buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReusableBufferReader<T extends Buffer & Referenceable<T>> extends BufferReader {
  private final T buffer;
  private final ReferenceManager<ReusableBufferReader<T>> referenceManager;

  public ReusableBufferReader(T buffer, ReferenceManager<ReusableBufferReader<T>> referenceManager) {
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
