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
import io.atomix.group.messaging.MessageProducer;

/**
 * Group message state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
abstract class MessageState implements AutoCloseable {
  protected final Commit<GroupCommands.Message> commit;
  protected final QueueState queue;
  private boolean complete;

  protected MessageState(Commit<GroupCommands.Message> commit, QueueState queue) {
    this.commit = commit;
    this.queue = queue;
  }

  /**
   * Returns the message queue.
   */
  public String queue() {
    return commit.operation().queue();
  }

  /**
   * Returns the message index.
   */
  public long index() {
    return commit.index();
  }

  /**
   * Returns the message session.
   */
  public ServerSession session() {
    return commit.session();
  }

  /**
   * Returns the message value.
   */
  public Object message() {
    return commit.operation().message();
  }

  /**
   * Returns the message delivery policy.
   */
  public MessageProducer.Execution execution() {
    return commit.operation().execution();
  }

  /**
   * Sends the message to the given member.
   */
  public abstract boolean send(MembersState members);

  /**
   * Replies to the message.
   */
  public abstract void reply(GroupCommands.Reply message);

  /**
   * Expires processing of the message.
   */
  public abstract void expire();

  /**
   * Sends a response back to the message submitter.
   */
  protected boolean sendReply(boolean succeeded, Object message) {
    if (!complete && session().state().active()) {
      session().publish("ack", new GroupCommands.Ack(commit.operation().member(), commit.operation().producer(), commit.operation().queue(), commit.operation().id(), succeeded, message));
      complete = true;
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    commit.close();
  }

}
