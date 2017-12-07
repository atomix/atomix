/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.storage.buffer;

import io.atomix.utils.concurrent.ReferenceManager;

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
  protected void compact(int from, int to, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer duplicate() {
    return new ReadOnlyBuffer(root, referenceManager);
  }

  @Override
  public Buffer acquire() {
    root.acquire();
    return this;
  }

  @Override
  public boolean release() {
    return root.release();
  }

  @Override
  public Buffer zero(int offset, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer zero(int offset) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer zero() {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeBoolean(int offset, boolean b) {
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
  public Buffer write(Bytes bytes, int offset, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(int offset, Bytes bytes, int srcOffset, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(byte[] bytes) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(byte[] bytes, int offset, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer write(int offset, byte[] bytes, int srcOffset, int length) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeByte(int b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeByte(int offset, int b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedByte(int b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedByte(int offset, int b) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeChar(char c) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeChar(int offset, char c) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeShort(short s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeShort(int offset, short s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedShort(int s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedShort(int offset, int s) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeMedium(int m) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeMedium(int offset, int m) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedMedium(int m) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedMedium(int offset, int m) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeInt(int i) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeInt(int offset, int i) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedInt(long i) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeUnsignedInt(int offset, long i) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeLong(long l) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeLong(int offset, long l) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeFloat(float f) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeFloat(int offset, float f) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeDouble(double d) {
    throw new ReadOnlyBufferException();
  }

  @Override
  public Buffer writeDouble(int offset, double d) {
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
