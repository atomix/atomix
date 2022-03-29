// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.log.partition.impl;

/**
 * Protocol message context.
 */
class LogMessageContext {
  final String appendSubject;
  final String consumeSubject;
  final String resetSubject;
  final String backupSubject;

  LogMessageContext(String prefix) {
    this.appendSubject = getSubject(prefix, "append");
    this.consumeSubject = getSubject(prefix, "consume");
    this.resetSubject = getSubject(prefix, "reset");
    this.backupSubject = getSubject(prefix, "backup");
  }

  private static String getSubject(String prefix, String type) {
    return String.format("%s-%s", prefix, type);
  }
}
