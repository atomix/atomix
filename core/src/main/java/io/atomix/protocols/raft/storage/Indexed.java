/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage;

import io.atomix.protocols.raft.storage.entry.Entry;
import io.atomix.protocols.raft.storage.util.StorageSerializer;
import io.atomix.util.buffer.BufferInput;
import io.atomix.util.buffer.BufferOutput;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Indexed log entry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Indexed<T extends Entry<T>> {
  private final long index;
  private final long term;
  private final T entry;
  private final int size;

  public Indexed(long index, long term, T entry, int size) {
    this.index = index;
    this.term = term;
    this.entry = entry;
    this.size = size;
  }

  /**
   * Returns the entry index.
   *
   * @return The entry index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the entry type.
   *
   * @return The entry type.
   */
  public Entry.Type<T> type() {
    return entry.type();
  }

  /**
   * Returns the indexed entry.
   *
   * @return The indexed entry.
   */
  public T entry() {
    return entry;
  }

  /**
   * Returns the serialized entry size.
   *
   * @return The serialized entry size.
   */
  public int size() {
    return size;
  }

  /**
   * Casts the entry to the given type.
   *
   * @param <T> The type to which to cast the entry.
   * @return The cast entry.
   */
  @SuppressWarnings("unchecked")
  public <T extends Entry<T>> Indexed<T> cast() {
    return (Indexed<T>) this;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index)
        .add("term", term)
        .add("entry", entry)
        .toString();
  }

  /**
   * Indexed entry serializer.
   */
  public static class Serializer implements StorageSerializer<Indexed> {
    @Override
    @SuppressWarnings("unchecked")
    public void writeObject(BufferOutput output, Indexed entry) {
      output.writeLong(entry.index);
      output.writeLong(entry.term);
      output.writeByte(entry.entry.type().id());
      entry.type().serializer().writeObject(output, entry.entry);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Indexed readObject(BufferInput input, Class<Indexed> type) {
      long index = input.readLong();
      long term = input.readLong();
      Entry.Type<?> entryType = Entry.Type.forId(input.readByte());
      return new Indexed(index, term, entryType.serializer().readObject(input, entryType.type()), (int) input.position());
    }
  }
}