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
package net.kuujo.copycat.raft.server.log;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.log.Entry;
import net.kuujo.copycat.util.ReferenceManager;

/**
 * Session entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class SessionEntry<T extends SessionEntry<T>> extends TimestampedEntry<T> {
  private long session;

  protected SessionEntry() {
  }

  protected SessionEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Sets the session ID.
   *
   * @param session The session ID.
   * @return The session entry.
   */
  @SuppressWarnings("unchecked")
  public T setSession(long session) {
    this.session = session;
    return (T) this;
  }

  /**
   * Returns the session ID.
   *
   * @return The session ID.
   */
  public long getSession() {
    return session;
  }

  @Override
  public int size() {
    return super.size() + Long.BYTES;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(session);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    session = buffer.readLong();
  }

}
