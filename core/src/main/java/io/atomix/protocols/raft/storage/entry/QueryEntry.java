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
package io.atomix.protocols.raft.storage.entry;

import io.atomix.util.ArraySizeHashPrinter;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Represents a state machine query.
 * <p>
 * The {@code QueryEntry} is a special entry that is typically not ever written to the Raft log.
 * Query entries are simply used to represent the context within which a query is applied to the
 * state machine. Query entry {@link #sequence() sequence} numbers and indexes
 * are used to sequence queries as they're applied to the user state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryEntry extends OperationEntry<QueryEntry> {

  public QueryEntry(long timestamp, long session, long sequence, byte[] bytes) {
    super(timestamp, session, sequence, bytes);
  }

  @Override
  public Type<QueryEntry> type() {
    return Type.QUERY;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("timestamp", timestamp)
        .add("session", session)
        .add("sequence", sequence)
        .add("query", ArraySizeHashPrinter.of(bytes))
        .toString();
  }
}