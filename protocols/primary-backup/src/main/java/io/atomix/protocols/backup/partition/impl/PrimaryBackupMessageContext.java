// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.partition.impl;

/**
 * Protocol message context.
 */
class PrimaryBackupMessageContext {
  private final String prefix;
  final String executeSubject;
  final String metadataSubject;
  final String backupSubject;
  final String restoreSubject;
  final String closeSubject;

  PrimaryBackupMessageContext(String prefix) {
    this.prefix = prefix;
    this.executeSubject = getSubject(prefix, "execute");
    this.metadataSubject = getSubject(prefix, "metadata");
    this.backupSubject = getSubject(prefix, "backup");
    this.restoreSubject = getSubject(prefix, "restore");
    this.closeSubject = getSubject(prefix, "close");
  }

  private static String getSubject(String prefix, String type) {
    return String.format("%s-%s", prefix, type);
  }

  /**
   * Returns the event subject for the given session.
   *
   * @param sessionId the session for which to return the event subject
   * @return the event subject for the given session
   */
  String eventSubject(long sessionId) {
    if (prefix == null) {
      return String.format("event-%d", sessionId);
    } else {
      return String.format("%s-event-%d", prefix, sessionId);
    }
  }
}
