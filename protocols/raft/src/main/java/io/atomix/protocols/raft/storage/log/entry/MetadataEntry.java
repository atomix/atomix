// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.storage.log.entry;

/**
 * Metadata entry.
 */
public class MetadataEntry extends SessionEntry {
  public MetadataEntry(long term, long timestamp, long session) {
    super(term, timestamp, session);
  }
}
