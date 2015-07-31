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
package net.kuujo.copycat.io.log;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ReferenceManager;

/**
 * Command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=1000)
public class TestEntry extends Entry<TestEntry> {
  private long term;
  private boolean remove;

  public TestEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the entry term.
   *
   * @param term The entry term.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public TestEntry setTerm(long term) {
    this.term = term;
    return this;
  }

  /**
   * Returns whether to remove the entry.
   *
   * @return Whether to remove the entry.
   */
  public boolean isRemove() {
    return remove;
  }

  /**
   * Sets whether to remove the entry.
   *
   * @param remove Whether to remove the entry.
   * @return The entry.
   */
  public TestEntry setRemove(boolean remove) {
    this.remove = remove;
    return this;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(term).writeBoolean(remove);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    term = buffer.readLong();
    remove = buffer.readBoolean();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, remove=%b]", getClass().getSimpleName(), getIndex(), term, remove);
  }

}
