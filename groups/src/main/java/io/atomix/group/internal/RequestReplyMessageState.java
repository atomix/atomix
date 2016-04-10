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
import io.atomix.group.messaging.MessageProducer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Request-reply message state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class RequestReplyMessageState extends MessageState {
  private int ack;
  private int fail;
  private List<Object> replies;

  public RequestReplyMessageState(Commit<GroupCommands.Message> commit, QueueState queue) {
    super(commit, queue);
  }

  @Override
  public boolean send(MembersState members) {
    if (commit.operation().delivery() == MessageProducer.Delivery.DIRECT) {
      MemberState member = members.get(commit.operation().member());
      if (member != null) {
        member.submit(this);
        return true;
      } else {
        sendReply(false, null);
        return false;
      }
    } else if (commit.operation().delivery() == MessageProducer.Delivery.RANDOM) {
      if (members.isEmpty()) {
        sendReply(false, null);
        return false;
      } else {
        members.get(new Random(commit.operation().id()).nextInt(members.size())).submit(this);
        return true;
      }
    } else if (commit.operation().delivery() == MessageProducer.Delivery.BROADCAST) {
      this.replies = new ArrayList<>(Collections.nCopies(members.size(), null));
      members.forEach(m -> m.submit(this));
      return true;
    } else {
      sendReply(false, null);
      return false;
    }
  }

  @Override
  public void reply(GroupCommands.Reply reply) {
    if (commit.operation().delivery() == MessageProducer.Delivery.DIRECT || commit.operation().delivery() == MessageProducer.Delivery.RANDOM) {
      sendReply(reply.succeeded(), reply.message());
    } else if (commit.operation().delivery() == MessageProducer.Delivery.BROADCAST) {
      if (reply.succeeded()) {
        ack++;
        replies.set(ack + fail, reply.message());
      } else {
        fail++;
      }

      if (ack + fail == replies.size()) {
        sendReply(fail == 0, replies);
        queue.close(this);
      }
    }
  }

  @Override
  public void expire() {
    if (commit.operation().delivery() == MessageProducer.Delivery.DIRECT || commit.operation().delivery() == MessageProducer.Delivery.RANDOM) {
      sendReply(false, null);
    } else if (commit.operation().delivery() == MessageProducer.Delivery.BROADCAST) {
      fail++;
      if (ack + fail == replies.size()) {
        sendReply(false, replies);
        queue.close(this);
      }
    }
  }

}
