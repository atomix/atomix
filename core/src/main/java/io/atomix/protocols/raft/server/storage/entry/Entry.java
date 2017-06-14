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
package io.atomix.protocols.raft.server.storage.entry;

import io.atomix.protocols.raft.server.storage.util.StorageSerializer;

/**
 * Stores a state change in a {@link io.atomix.protocols.raft.server.storage.Log}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Entry<T extends Entry<T>> {

  /**
   * Entry type.
   */
  public static class Type<T extends Entry<T>> {
    public static final Type<OpenSessionEntry>    OPEN_SESSION = new Type<>(0x01, OpenSessionEntry.class, new OpenSessionEntry.Serializer());
    public static final Type<KeepAliveEntry>        KEEP_ALIVE = new Type<>(0x02, KeepAliveEntry.class, new KeepAliveEntry.Serializer());
    public static final Type<CloseSessionEntry>  CLOSE_SESSION = new Type<>(0x03, CloseSessionEntry.class, new CloseSessionEntry.Serializer());
    public static final Type<QueryEntry>                 QUERY = new Type<>(0x04, QueryEntry.class, null);
    public static final Type<CommandEntry>             COMMAND = new Type<>(0x05, CommandEntry.class, new CommandEntry.Serializer());
    public static final Type<ConfigurationEntry> CONFIGURATION = new Type<>(0x06, ConfigurationEntry.class, new ConfigurationEntry.Serializer());
    public static final Type<InitializeEntry>       INITIALIZE = new Type<>(0x07, InitializeEntry.class, new InitializeEntry.Serializer());
    public static final Type<MetadataEntry>           METADATA = new Type<>(0x08, MetadataEntry.class, null);

    /**
     * Returns the entry type for the given ID.
     *
     * @param id The entry type ID.
     * @return The entry type.
     */
    public static Type<?> forId(int id) {
      switch (id) {
        case 0x01:
          return OPEN_SESSION;
        case 0x02:
          return KEEP_ALIVE;
        case 0x03:
          return CLOSE_SESSION;
        case 0x04:
          return QUERY;
        case 0x05:
          return COMMAND;
        case 0x06:
          return CONFIGURATION;
        case 0x07:
          return INITIALIZE;
        case 0x08:
          return METADATA;
        default:
          throw new IllegalArgumentException("invalid entry type ID: " + id);
      }
    }

    private final int id;
    private final Class<T> type;
    private final Serializer<T> serializer;

    public Type(int id, Class<T> type, Serializer<T> serializer) {
      this.id = id;
      this.type = type;
      this.serializer = serializer;
    }

    /**
     * Returns the entry type ID.
     *
     * @return The entry type ID.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the entry class.
     *
     * @return The entry class.
     */
    public Class type() {
      return type;
    }

    /**
     * Returns the entry serializer.
     *
     * @return The entry serializer.
     */
    public Serializer<T> serializer() {
      return serializer;
    }
  }

  /**
   * Returns the entry type.
   *
   * @return The entry type.
   */
  public abstract Type<T> type();

  /**
   * Entry serializer.
   *
   * @param <T> The entry type.
   */
  public interface Serializer<T extends Entry> extends StorageSerializer<T> {
  }
}
