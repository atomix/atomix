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

import java.util.HashMap;
import java.util.Map;

/**
 * Message queue state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class QueueState implements AutoCloseable {
  private final Map<Long, MessageState> messages = new HashMap<>();
  private final MembersState members;

  QueueState(MembersState members) {
    this.members = members;
  }

  /**
   * Submits the given commit to the queue.
   */
  public void submit(MessageState message) {
    if (message.send(members)) {
      messages.put(message.index(), message);
    } else {
      message.close();
    }
  }

  /**
   * Replies to the given message.
   */
  public void reply(GroupCommands.Reply reply) {
    MessageState message = messages.get(reply.id());
    if (message != null) {
      MemberState member = members.get(reply.member());
      if (member != null) {
        member.reply(message, reply);
      }
    }
  }

  /**
   * Closes the given message.
   */
  public void close(MessageState message) {
    messages.remove(message.index());
    message.close();
  }

  @Override
  public void close() {
    messages.values().forEach(MessageState::close);
  }

}
