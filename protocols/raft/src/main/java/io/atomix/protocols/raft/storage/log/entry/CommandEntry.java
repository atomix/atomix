// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log.entry;

import io.atomix.primitive.operation.PrimitiveOperation;

/**
 * Stores a state machine command.
 * <p>
 * The {@code CommandEntry} is used to store an individual state machine command from an individual
 * client along with information relevant to sequencing the command in the server state machine.
 */
public class CommandEntry extends OperationEntry {
  public CommandEntry(long term, long timestamp, long session, long sequence, PrimitiveOperation operation) {
    super(term, timestamp, session, sequence, operation);
  }
}
