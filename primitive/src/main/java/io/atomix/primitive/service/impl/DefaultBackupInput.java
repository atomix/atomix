// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.service.impl;

import io.atomix.primitive.service.BackupInput;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.BufferInput;
import io.atomix.storage.buffer.Bytes;
import io.atomix.utils.serializer.Serializer;

import java.nio.charset.Charset;

/**
 * Default backup input.
 */
public class DefaultBackupInput implements BackupInput {
  private final BufferInput<?> input;
  private final Serializer serializer;

  public DefaultBackupInput(BufferInput<?> input, Serializer serializer) {
    this.input = input;
    this.serializer = serializer;
  }

  @Override
  public <U> U readObject() {
    return input.readObject(bytes -> bytes != null ? serializer.decode(bytes) : null);
  }

  @Override
  public int position() {
    return input.position();
  }

  @Override
  public int remaining() {
    return input.remaining();
  }

  @Override
  public boolean hasRemaining() {
    return input.hasRemaining();
  }

  @Override
  public BackupInput skip(int bytes) {
    input.skip(bytes);
    return this;
  }

  @Override
  public BackupInput read(Bytes bytes) {
    input.read(bytes);
    return this;
  }

  @Override
  public BackupInput read(byte[] bytes) {
    input.read(bytes);
    return this;
  }

  @Override
  public BackupInput read(Bytes bytes, int offset, int length) {
    input.read(bytes, offset, length);
    return this;
  }

  @Override
  public BackupInput read(byte[] bytes, int offset, int length) {
    input.read(bytes, offset, length);
    return this;
  }

  @Override
  public BackupInput read(Buffer buffer) {
    input.read(buffer);
    return this;
  }

  @Override
  public int readByte() {
    return input.readByte();
  }

  @Override
  public int readUnsignedByte() {
    return input.readUnsignedByte();
  }

  @Override
  public char readChar() {
    return input.readChar();
  }

  @Override
  public short readShort() {
    return input.readShort();
  }

  @Override
  public int readUnsignedShort() {
    return input.readUnsignedShort();
  }

  @Override
  public int readMedium() {
    return input.readMedium();
  }

  @Override
  public int readUnsignedMedium() {
    return input.readUnsignedMedium();
  }

  @Override
  public int readInt() {
    return input.readInt();
  }

  @Override
  public long readUnsignedInt() {
    return input.readUnsignedInt();
  }

  @Override
  public long readLong() {
    return input.readLong();
  }

  @Override
  public float readFloat() {
    return input.readFloat();
  }

  @Override
  public double readDouble() {
    return input.readDouble();
  }

  @Override
  public boolean readBoolean() {
    return input.readBoolean();
  }

  @Override
  public String readString() {
    return input.readString();
  }

  @Override
  public String readString(Charset charset) {
    return input.readString(charset);
  }

  @Override
  public String readUTF8() {
    return input.readUTF8();
  }

  @Override
  public void close() {
    input.close();
  }
}
