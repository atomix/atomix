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

import net.kuujo.copycat.util.ReferenceManager;

import java.nio.ReadOnlyBufferException;

/**
 * Read-only buffer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ReadOnlyBuffer extends AbstractBuffer {
  private final Buffer root;

  public ReadOnlyBuffer(Buffer buffer, ReferenceManager<Buffer> referenceManager) {
    super(buffer.bytes(), referenceManager);
    this.root = buffer;
  }

  @Override
  public boolean isDirect() {
    return root.isDirect();
  }


  @Override
  public boolean isFile() {
    return root.isFile();
  }

  @Override
  public boolean isReadOnly() {
    return true;
  }

  @Override
  public Buffer compact() {
    throw new ReadOnlyBufferException();
  }

  @Override
  protected void compact(long from, long to, long length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer acquire() {
    root.acquire();
    return this;
  }

  @Override
  public void release() {
    root.release();
  }

  @Override
  public Buffer zero(long offset, long length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer zero(long offset) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer zero() {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeBoolean(long offset, boolean b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(Buffer buffer) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(Bytes bytes) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(Bytes bytes, long offset, long length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(long offset, Bytes bytes, long srcOffset, long length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(byte[] bytes) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(byte[] bytes, long offset, long length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(long offset, byte[] bytes, long srcOffset, long length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeByte(int b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeByte(long offset, int b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedByte(int b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedByte(long offset, int b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeChar(char c) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeChar(long offset, char c) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeShort(short s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeShort(long offset, short s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedShort(int s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedShort(long offset, int s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeMedium(int m) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeMedium(long offset, int m) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedMedium(int m) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedMedium(long offset, int m) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeInt(int i) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeInt(long offset, int i) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedInt(long i) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedInt(long offset, long i) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeLong(long l) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeLong(long offset, long l) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeFloat(float f) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeFloat(long offset, float f) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeDouble(double d) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeDouble(long offset, double d) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeBoolean(boolean b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUTF8(String s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer flush() {
    throw new ReadOnlyBufferException();
  }

  @Override
  public void close() {
    root.release();
  }

}
