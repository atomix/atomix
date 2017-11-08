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
package io.atomix.protocols.raft.protocol.messaging;

import io.atomix.cluster.messaging.MessageSubject;

/**
 * Protocol message context.
 */
class RaftMessageContext {
  private final String prefix;
  final MessageSubject heartbeatSubject;
  final MessageSubject openSessionSubject;
  final MessageSubject closeSessionSubject;
  final MessageSubject keepAliveSubject;
  final MessageSubject querySubject;
  final MessageSubject commandSubject;
  final MessageSubject metadataSubject;
  final MessageSubject joinSubject;
  final MessageSubject leaveSubject;
  final MessageSubject configureSubject;
  final MessageSubject reconfigureSubject;
  final MessageSubject installSubject;
  final MessageSubject transferSubject;
  final MessageSubject pollSubject;
  final MessageSubject voteSubject;
  final MessageSubject appendSubject;

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

  private static MessageSubject getSubject(String prefix, String type) {
    if (prefix == null) {
      return new MessageSubject(type);
    } else {
      return new MessageSubject(String.format("%s-%s", prefix, type));
    }
  }

  /**
   * Returns the publish subject for the given session.
   *
   * @param sessionId the session for which to return the publish subject
   * @return the publish subject for the given session
   */
  MessageSubject publishSubject(long sessionId) {
    if (prefix == null) {
      return new MessageSubject(String.format("publish-%d", sessionId));
    } else {
      return new MessageSubject(String.format("%s-publish-%d", prefix, sessionId));
    }
  }

  /**
   * Returns the reset subject for the given session.
   *
   * @param sessionId the session for which to return the reset subject
   * @return the reset subject for the given session
   */
  MessageSubject resetSubject(long sessionId) {
    if (prefix == null) {
      return new MessageSubject(String.format("reset-%d", sessionId));
    } else {
      return new MessageSubject(String.format("%s-reset-%d", prefix, sessionId));
    }
  }
}