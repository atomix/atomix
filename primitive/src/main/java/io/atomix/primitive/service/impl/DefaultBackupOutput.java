/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.service.impl;

import io.atomix.primitive.service.BackupOutput;
import io.atomix.storage.buffer.Buffer;
import io.atomix.storage.buffer.BufferOutput;
import io.atomix.storage.buffer.Bytes;
import io.atomix.utils.serializer.Serializer;

import java.nio.charset.Charset;

/**
 * Default backup output.
 */
public class DefaultBackupOutput implements BackupOutput {
  private final BufferOutput<?> output;
  private final Serializer serializer;

  public DefaultBackupOutput(BufferOutput<?> output, Serializer serializer) {
    this.output = output;
    this.serializer = serializer;
  }

  @Override
  public <U> BackupOutput writeObject(U object) {
    output.writeObject(object, o -> o != null ? serializer.encode(o) : null);
    return this;
  }

  @Override
  public BackupOutput write(Bytes bytes) {
    output.write(bytes);
    return this;
  }

  @Override
  public BackupOutput write(byte[] bytes) {
    output.write(bytes);
    return this;
  }

  @Override
  public BackupOutput write(Bytes bytes, int offset, int length) {
    output.write(bytes, offset, length);
    return this;
  }

  @Override
  public BackupOutput write(byte[] bytes, int offset, int length) {
    output.write(bytes, offset, length);
    return this;
  }

  @Override
  public BackupOutput write(Buffer buffer) {
    output.write(buffer);
    return this;
  }

  @Override
  public BackupOutput writeByte(int b) {
    output.writeByte(b);
    return this;
  }

  @Override
  public BackupOutput writeUnsignedByte(int b) {
    output.writeUnsignedByte(b);
    return this;
  }

  @Override
  public BackupOutput writeChar(char c) {
    output.writeChar(c);
    return this;
  }

  @Override
  public BackupOutput writeShort(short s) {
    output.writeShort(s);
    return this;
  }

  @Override
  public BackupOutput writeUnsignedShort(int s) {
    output.writeUnsignedShort(s);
    return this;
  }

  @Override
  public BackupOutput writeMedium(int m) {
    output.writeMedium(m);
    return this;
  }

  @Override
  public BackupOutput writeUnsignedMedium(int m) {
    output.writeUnsignedMedium(m);
    return this;
  }

  @Override
  public BackupOutput writeInt(int i) {
    output.writeInt(i);
    return this;
  }

  @Override
  public BackupOutput writeUnsignedInt(long i) {
    output.writeUnsignedInt(i);
    return this;
  }

  @Override
  public BackupOutput writeLong(long l) {
    output.writeLong(l);
    return this;
  }

  @Override
  public BackupOutput writeFloat(float f) {
    output.writeFloat(f);
    return this;
  }

  @Override
  public BackupOutput writeDouble(double d) {
    output.writeDouble(d);
    return this;
  }

  @Override
  public BackupOutput writeBoolean(boolean b) {
    output.writeBoolean(b);
    return this;
  }

  @Override
  public BackupOutput writeString(String s) {
    output.writeString(s);
    return this;
  }

  @Override
  public BackupOutput writeString(String s, Charset charset) {
    output.writeString(s, charset);
    return this;
  }

  @Override
  public BackupOutput writeUTF8(String s) {
    output.writeUTF8(s);
    return this;
  }

  @Override
  public BackupOutput flush() {
    output.flush();
    return this;
  }

  @Override
  public void close() {
    output.close();
  }
}
