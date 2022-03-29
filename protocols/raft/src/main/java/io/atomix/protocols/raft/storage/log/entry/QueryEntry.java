// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.primitive.operation.PrimitiveOperation;

/**
 * Represents a state machine query.
 * <p>
 * The {@code QueryEntry} is a special entry that is typically not ever written to the Raft log.
 * Query entries are simply used to represent the context within which a query is applied to the
 * state machine. Query entry {@link #sequenceNumber() sequence} numbers and indexes
 * are used to sequence queries as they're applied to the user state machine.
 */
public class QueryEntry extends OperationEntry {
  public QueryEntry(long term, long timestamp, long session, long sequence, PrimitiveOperation operation) {
    super(term, timestamp, session, sequence, operation);
  }
}
