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

import java.nio.ByteOrder;

/**
 * Byte order swapped buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SwappedBuffer extends AbstractBuffer {
  private final Buffer root;

  SwappedBuffer(Buffer root, Bytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
    this.root = root;
  }

  public SwappedBuffer(Buffer buffer, long offset, long initialCapacity, long maxCapacity, ReferenceManager<Buffer> referenceManager) {
    super(buffer.bytes().order(buffer.order() == ByteOrder.BIG_ENDIAN ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN), offset, initialCapacity, maxCapacity, referenceManager);
    this.root = buffer instanceof SwappedBuffer ? ((SwappedBuffer) buffer).root : buffer;
  }

  /**
   * Returns the root buffer.
   *
   * @return The root buffer.
   */
  public Buffer root() {
    return root;
  }

  @Override
  protected Buffer createView(ReferenceManager<Buffer> referenceManager) {
    return new SwappedBuffer(root, bytes, referenceManager);
  }

}
