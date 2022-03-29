// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.partition.impl;

/**
 * Protocol message context.
 */
class RaftMessageContext {
  private final String prefix;
  final String heartbeatSubject;
  final String openSessionSubject;
  final String closeSessionSubject;
  final String keepAliveSubject;
  final String querySubject;
  final String commandSubject;
  final String metadataSubject;
  final String joinSubject;
  final String leaveSubject;
  final String configureSubject;
  final String reconfigureSubject;
  final String installSubject;
  final String transferSubject;
  final String pollSubject;
  final String voteSubject;
  final String appendSubject;

  RaftMessageContext(String prefix) {
    this.prefix = prefix;
    this.heartbeatSubject = getSubject(prefix, "heartbeat");
    this.openSessionSubject = getSubject(prefix, "open");
    this.closeSessionSubject = getSubject(prefix, "close");
    this.keepAliveSubject = getSubject(prefix, "keep-alive");
    this.querySubject = getSubject(prefix, "query");
    this.commandSubject = getSubject(prefix, "command");
    this.metadataSubject = getSubject(prefix, "metadata");
    this.joinSubject = getSubject(prefix, "join");
    this.leaveSubject = getSubject(prefix, "leave");
    this.configureSubject = getSubject(prefix, "configure");
    this.reconfigureSubject = getSubject(prefix, "reconfigure");
    this.installSubject = getSubject(prefix, "install");
    this.transferSubject = getSubject(prefix, "transfer");
    this.pollSubject = getSubject(prefix, "poll");
    this.voteSubject = getSubject(prefix, "vote");
    this.appendSubject = getSubject(prefix, "append");
  }

  private static String getSubject(String prefix, String type) {
    if (prefix == null) {
      return type;
    } else {
      return String.format("%s-%s", prefix, type);
    }
  }

  /**
   * Returns the publish subject for the given session.
   *
   * @param sessionId the session for which to return the publish subject
   * @return the publish subject for the given session
   */
  String publishSubject(long sessionId) {
    if (prefix == null) {
      return String.format("publish-%d", sessionId);
    } else {
      return String.format("%s-publish-%d", prefix, sessionId);
    }
  }

  /**
   * Returns the reset subject for the given session.
   *
   * @param sessionId the session for which to return the reset subject
   * @return the reset subject for the given session
   */
  String resetSubject(long sessionId) {
    if (prefix == null) {
      return String.format("reset-%d", sessionId);
    } else {
      return String.format("%s-reset-%d", prefix, sessionId);
    }
  }
}
