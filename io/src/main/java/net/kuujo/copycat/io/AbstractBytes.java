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

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteOrder;

/**
 * Abstract bytes implementation.
 * <p>
 * This class provides common state and bounds checking functionality for all {@link net.kuujo.copycat.io.Bytes} implementations.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractBytes implements Bytes {
  private boolean open = true;
  private SwappedBytes swap;

  /**
   * Checks whether the block is open.
   */
  protected void checkOpen() {
    if (!open)
      throw new IllegalStateException("bytes not open");
  }

  /**
   * Checks that the offset is within the bounds of the buffer.
   */
  protected void checkOffset(long offset) {
    checkOpen();
    if (offset < 0 || offset > size())
      throw new IndexOutOfBoundsException();
  }

  /**
   * Checks bounds for a read.
   */
  protected long checkRead(long offset, long length) {
    checkOffset(offset);
    long position = offset + length;
    if (position > size())
      throw new BufferUnderflowException();
    return position;
  }

  /**
   * Checks bounds for a write.
   */
  protected long checkWrite(long offset, long length) {
    checkOffset(offset);
    long position = offset + length;
    if (position > size())
      throw new BufferOverflowException();
    return position;
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public boolean isFile() {
    return false;
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.BIG_ENDIAN;
  }

  @Override
  public Bytes order(ByteOrder order) {
    if (order == null)
      throw new NullPointerException("order cannot be null");
    if (order == order())
      return this;
    if (swap != null)
      return swap;
    swap = new SwappedBytes(this);
    return swap;
  }

  @Override
  public void close() {
    open = false;
  }

}
