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
package net.kuujo.copycat.log.compaction;

import net.kuujo.copycat.log.LogException;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * File-based lookup strategy implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileLookupStrategy extends AbstractLookupStrategy {
  private final FileChannel channel;
  private final ByteBuffer keyBuffer = ByteBuffer.allocateDirect(4);
  private final ByteBuffer indexBuffer = ByteBuffer.allocateDirect(8);

  public FileLookupStrategy(File file, int entryLimit) {
    super(entryLimit / 2);
    try {
      this.channel = FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    } catch (IOException e) {
      throw new LogException(e);
    }
  }

  @Override
  protected int readKey(int position) {
    try {
      if (channel.read(keyBuffer, position) > 0) {
        keyBuffer.flip();
        return keyBuffer.getInt();
      }
    } catch (IOException e) {
      throw new LogException(e);
    } finally {
      keyBuffer.reset();
    }
    return 0;
  }

  @Override
  protected void writeKey(int position, int hash) {
    try {
      keyBuffer.putInt(hash);
      keyBuffer.flip();
      channel.write(keyBuffer, position);
    } catch (IOException e) {
      throw new LogException(e);
    } finally {
      keyBuffer.reset();
    }
  }

  @Override
  protected long readIndex(int position) {
    try {
      if (channel.read(indexBuffer, position) > 0) {
        indexBuffer.flip();
        return indexBuffer.getLong();
      }
    } catch (IOException e) {
      throw new LogException(e);
    } finally {
      indexBuffer.reset();
    }
    return 0;
  }

  @Override
  protected void writeIndex(int position, long index) {
    try {
      indexBuffer.putLong(index);
      indexBuffer.flip();
      channel.write(indexBuffer, position);
    } catch (IOException e) {
      throw new LogException(e);
    } finally {
      indexBuffer.reset();
    }
  }

}
