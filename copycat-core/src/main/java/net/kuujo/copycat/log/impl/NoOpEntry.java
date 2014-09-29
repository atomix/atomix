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
package net.kuujo.copycat.log.impl;

import net.kuujo.copycat.log.Buffer;
import net.kuujo.copycat.log.EntryReader;
import net.kuujo.copycat.log.EntryType;
import net.kuujo.copycat.log.EntryWriter;

/**
 * No-op entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@EntryType(id=0, reader=NoOpEntry.Reader.class, writer=NoOpEntry.Writer.class)
public class NoOpEntry extends RaftEntry {

  private NoOpEntry() {
    super();
  }

  public NoOpEntry(long term) {
    super(term);
  }

  public static class Reader implements EntryReader<NoOpEntry> {
    @Override
    public NoOpEntry readEntry(Buffer buffer) {
      NoOpEntry entry = new NoOpEntry();
      entry.term = buffer.getLong();
      return entry;
    }
  }

  public static class Writer implements EntryWriter<NoOpEntry> {
    @Override
    public void writeEntry(NoOpEntry entry, Buffer buffer) {
      buffer.appendLong(entry.term);
    }
  }

}
