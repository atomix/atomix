/*
 * Copyright 2017-present Open Networking Foundation
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
