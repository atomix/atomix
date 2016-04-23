/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.internal;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.group.messaging.internal.GroupMessage;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Group member state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class MemberState implements AutoCloseable {
  private final Commit<GroupCommands.Join> commit;
  private final long index;
  private final String memberId;
  private final boolean persistent;
  private ServerSession session;
  private Object metadata;
  private final Map<Long, MessageState> messages = new LinkedHashMap<>();

  MemberState(Commit<GroupCommands.Join> commit) {
    this.commit = commit;
    this.index = commit.index();
    this.memberId = commit.operation().member();
    this.persistent = commit.operation().persist();
    this.session = commit.session();
    this.metadata = commit.operation().metadata();
  }

  /**
   * Returns the member index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the member ID.
   */
  public String id() {
    return memberId;
  }

  /**
   * Returns group member info.
   */
  public GroupMemberInfo info() {
    return new GroupMemberInfo(index, memberId, metadata);
  }

  /**
   * Returns the member session.
   */
  public ServerSession session() {
    return session;
  }

  /**
   * Sets the member session.
   */
  public void setSession(ServerSession session) {
    this.session = session;
    if (session != null && session.state().active()) {
      for (MessageState message : messages.values()) {
        session.publish("message", new GroupMessage<>(message.index(), memberId, message.queue(), message.message()));
      }
    }
  }

  /**
   * Returns a boolean indicating whether the member is persistent.
   */
  public boolean persistent() {
    return persistent;
  }

  public Object metadata() {
    return metadata;
  }

  /**
   * Submits the given message to be processed by the member.
   */
  public void submit(MessageState message) {
    messages.put(message.index(), message);
    if (session != null && session.state().active()) {
      session.publish("message", new GroupMessage<>(message.index(), memberId, message.queue(), message.message()));
    }
  }

  /**
   * Replies to the message.
   */
  public void reply(MessageState message, GroupCommands.Reply reply) {
    messages.remove(message.index());
    message.reply(reply);
  }

  @Override
  public void close() {
    messages.values().forEach(MessageState::expire);
    commit.close();
  }

  @Override
  public int hashCode() {
    return commit.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof MemberState && ((MemberState) object).id().equals(id());
  }

}
