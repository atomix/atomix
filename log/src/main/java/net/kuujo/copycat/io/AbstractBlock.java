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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Abstract block implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractBlock<T extends AbstractBlock<T>> implements Block, Referenceable<T> {
  private final AtomicInteger referenceCounter = new AtomicInteger();
  private final ReferenceManager<T> referenceManager;
  private final ReusableBufferReaderPool<T> readerPool;
  private final ReusableBufferWriterPool<T> writerPool;
  private final int index;
  private long capacity;
  private long position;
  private long limit;
  private long mark;

  @SuppressWarnings("unchecked")
  public AbstractBlock(int index, long capacity, ReferenceManager<T> manager) {
    if (manager == null)
      throw new NullPointerException("manager cannot be null");
    this.referenceManager = manager;
    this.readerPool = new ReusableBufferReaderPool<>((T) this);
    this.writerPool = new ReusableBufferWriterPool<>((T) this);
    this.index = index;
    this.capacity = capacity;
    this.position = 0;
    this.mark = position;
    this.limit = capacity;
  }

  @Override
  public int index() {
    return index;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T acquire() {
    referenceCounter.incrementAndGet();
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void release() {
    if (referenceCounter.decrementAndGet() == 0)
      referenceManager.release((T) this);
  }

  @Override
  public int references() {
    return referenceCounter.get();
  }

  @Override
  public BufferReader reader() {
    return readerPool.acquire();
  }

  @Override
  public BufferWriter writer() {
    return writerPool.acquire();
  }

  @Override
  public long capacity() {
    return capacity;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T position(long position) {
    if (position > capacity)
      throw new IllegalArgumentException("Position cannot be greater than block capacity");
    this.position = position;
    return (T) this;
  }

  @Override
  public long limit() {
    return limit;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T limit(long limit) {
    if (limit > capacity)
      throw new IllegalArgumentException("Limit cannot be greater than block capacity");
    return (T) this;
  }

  @Override
  public long remaining() {
    return limit - position;
  }

  @Override
  public boolean hasRemaining() {
    return limit - position > 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T mark(long mark) {
    checkBounds(mark);
    this.mark = mark;
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T rewind() {
    position = 0;
    mark = position;
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T reset() {
    position = mark;
    return (T) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T clear() {
    position = 0;
    limit = capacity;
    mark = position;
    return (T) this;
  }

  /**
   * Checks the bounds of the given offset.
   */
  protected void checkBounds(long offset) {
    if (offset > capacity)
      throw new IllegalArgumentException("Offset cannot be greater than the block capacity");
  }

  /**
   * Returns the current position and increments the position by the given number of bytes.
   */
  protected long incrementPosition(int bytes) {
    long position = this.position;
    this.position += bytes;
    return position;
  }

  @Override
  public char readChar() {
    return readChar(incrementPosition(Character.BYTES));
  }

  @Override
  public short readShort() {
    return readShort(incrementPosition(Short.BYTES));
  }

  @Override
  public int readInt() {
    return readInt(incrementPosition(Integer.BYTES));
  }

  @Override
  public long readLong() {
    return readLong(incrementPosition(Long.BYTES));
  }

  @Override
  public float readFloat() {
    return readFloat(incrementPosition(Float.BYTES));
  }

  @Override
  public double readDouble() {
    return readDouble(incrementPosition(Double.BYTES));
  }

  @Override
  public boolean readBoolean() {
    return readBoolean(incrementPosition(1));
  }

  @Override
  public Buffer writeChar(char c) {
    return writeChar(incrementPosition(Character.BYTES), c);
  }

  @Override
  public Buffer writeShort(short s) {
    return writeShort(incrementPosition(Short.BYTES), s);
  }

  @Override
  public Buffer writeInt(int i) {
    return writeInt(incrementPosition(Integer.BYTES), i);
  }

  @Override
  public Buffer writeLong(long l) {
    return writeLong(incrementPosition(Long.BYTES), l);
  }

  @Override
  public Buffer writeFloat(float f) {
    return writeFloat(incrementPosition(Float.BYTES), f);
  }

  @Override
  public Buffer writeDouble(double d) {
    return writeDouble(incrementPosition(Double.BYTES), d);
  }

  @Override
  public Buffer writeBoolean(boolean b) {
    return writeBoolean(incrementPosition(1), b);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void close() {
    release();
  }

}
