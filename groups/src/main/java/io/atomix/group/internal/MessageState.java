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
final class MessageState implements AutoCloseable {
  private final Commit<GroupCommands.Submit> commit;
  private final QueueState queue;
  private int members;
  private int ack;
  private int fail;
  private boolean complete;

  MessageState(Commit<GroupCommands.Submit> commit, QueueState queue) {
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
   * Submits the message.
   */
  public void submit(MemberState member) {
    members = 1;
    member.submit(this);
  }

  /**
   * Submits the message.
   */
  public boolean submit(MembersState members) {
    if (commit.operation().member() != null) {
      MemberState member = members.get(commit.operation().member());
      if (member != null) {
        member.submit(this);
        return true;
      } else {
        sendFail();
        return false;
      }
    } else if (commit.operation().dispatchPolicy() == MessageProducer.DispatchPolicy.RANDOM) {
      if (members.isEmpty()) {
        sendFail();
        return false;
      } else {
        members.select(index()).submit(this);
        return true;
      }
    } else if (commit.operation().dispatchPolicy() == MessageProducer.DispatchPolicy.BROADCAST) {
      this.members = members.size();
      members.forEach(m -> m.submit(this));
      return true;
    } else {
      sendFail();
      return false;
    }
  }

  /**
   * Succeeds processing of the message.
   */
  public void ack() {
    ack++;
    if (complete()) {
      queue.close(this);
    }
  }

  /**
   * Sends an ack message back to the message submitter.
   */
  private boolean sendAck() {
    if (!complete && session().state().active()) {
      session().publish("ack", commit.operation());
      complete = true;
      return true;
    }
    return false;
  }

  /**
   * Fails processing of the message.
   */
  public void fail() {
    fail++;
    if (complete()) {
      queue.close(this);
    }
  }

  /**
   * Sends a fail message back to the message submitter.
   */
  private boolean sendFail() {
    if (!complete && session().state().active()) {
      session().publish("fail", commit.operation());
      complete = true;
      return true;
    }
    return false;
  }

  /**
   * Attempts to complete the message.
   */
  private boolean complete() {
    if (commit.operation().member() != null) {
      return sendAck();
    } else if (commit.operation().dispatchPolicy() == MessageProducer.DispatchPolicy.RANDOM) {
      return sendAck();
    } else if (commit.operation().dispatchPolicy() == MessageProducer.DispatchPolicy.BROADCAST) {
      if (ack + fail == members) {
        if (commit.operation().deliveryPolicy() == MessageProducer.DeliveryPolicy.ALL) {
          if (fail == 0) {
            return sendAck();
          } else {
            return sendFail();
          }
        } else if (commit.operation().deliveryPolicy() == MessageProducer.DeliveryPolicy.ONE) {
          if (ack >= 1) {
            return sendAck();
          } else {
            return sendFail();
          }
        } else if (commit.operation().deliveryPolicy() == MessageProducer.DeliveryPolicy.NONE) {
          return sendAck();
        } else {
          return sendFail();
        }
      } else {
        return false;
      }
    } else {
      return sendFail();
    }
  }

  @Override
  public void close() {
    commit.close();
  }

}
