/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.log.internal;

import net.kuujo.copycat.log.EntryType;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * No-op entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=2, serializer=NoOpEntry.Serializer.class)
public class NoOpEntry extends CopycatEntry {

  private NoOpEntry() {
    super();
  }

  public NoOpEntry(long term) {
    super(term);
  }

  @Override
  public String toString() {
    return String.format("NoOpEntry[term=%d]", term);
  }

  /**
   * No-op entry serializer.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public static class Serializer extends com.esotericsoftware.kryo.Serializer<NoOpEntry> {
    @Override
    public NoOpEntry read(Kryo kryo, Input input, Class<NoOpEntry> type) {
      NoOpEntry entry = new NoOpEntry();
      entry.term = input.readLong();
      return entry;
    }
    @Override
    public void write(Kryo kryo, Output output, NoOpEntry entry) {
      output.writeLong(entry.term);
    }
  }

}
