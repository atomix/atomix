/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.coordination;

import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.coordination.state.GroupCommands;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * A {@link DistributedGroup} member representing a member of the group controlled by the
 * local process.
 * <p>
 * Local group members can only be acquired by {@link DistributedGroup#join() joining} a membership
 * group. Local members provide the interface necessary to set member properties, receive messages,
 * and react to the election of the member as the group leader.
 * <p>
 * To receive messages sent to the joined member of the group, register a message consumer. Messages
 * sent to the member are associated with a {@link String} topic, and separate handlers can be registered
 * for each topic supported by the local member:
 * <pre>
 *   {@code
 *   LocalGroupMember member = group.join().get();
 *   member.onMessage("foo", message -> System.out.println("received: " + message));
 *   }
 * </pre>
 * Membership groups automatically elect a leader for the group at all times. To determine when the local group
 * member has been elected leader, register a leader {@link #onElection(Consumer) election listener}:
 * <pre>
 *   {@code
 *   LocalGroupMember member = group.join().get();
 *   group.onElection(term -> {
 *     System.out.println("Elected leader for term " + term);
 *   });
 *   }
 * </pre>
 * The leader is guaranteed to be unique within a given {@link GroupElection#term() term}. However, once
 * the member is elected leader, it is not guaranteed to remain the leader of the group until failure. In the
 * event that the local group instance is partitioned from the rest of the cluster, the member may be removed
 * from leadership and another member of the group may be elected. Users should use the provided {@code term}
 * for fencing when interacting with external resources.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LocalGroupMember extends GroupMember {
  private final Map<String, MessageListenerHolder> messageListeners = new ConcurrentHashMap<>();
  private final Listeners<GroupTask<Object>> taskListeners = new Listeners<>();
  private final GroupConnection connection;

  LocalGroupMember(GroupMemberInfo info, DistributedGroup group) {
    super(info, group);
    connection = new GroupConnection(info.memberId(), info.address(), group.connections) {
      @Override
      @SuppressWarnings("unchecked")
      public <T, U> CompletableFuture<U> send(String topic, T message) {
        CompletableFuture<U> future = new CompletableFuture<>();
        handleMessage(new GroupMessage(memberId, topic, message).setFuture(future));
        return future;
      }
    };
  }

  /**
   * Registers a consumer for messages sent to the local member.
   * <p>
   * The provided message consumer will be called when a message sent to the local member
   * is received for the given {@code topic}.
   *
   * @param topic The message topic.
   * @param consumer The message consumer.
   * @param <T> The message type.
   * @return The message listener.
   */
  @SuppressWarnings("unchecked")
  public <T> Listener<GroupMessage<T>> onMessage(String topic, Consumer<GroupMessage<T>> consumer) {
    MessageListenerHolder listener = new MessageListenerHolder(consumer);
    messageListeners.put(topic, listener);
    return listener;
  }

  /**
   * Handles a message to the member.
   */
  void handleMessage(GroupMessage message) {
    MessageListenerHolder listener = messageListeners.get(message.topic());
    if (listener != null) {
      listener.accept(message);
    }
  }

  /**
   * Registers a consumer for tasks send to the local member.
   *
   * @param consumer The task consumer.
   * @param <T> The task type.
   * @return The task listener.
   */
  @SuppressWarnings("unchecked")
  public <T> Listener<GroupTask<T>> onTask(Consumer<GroupTask<T>> consumer) {
    return (Listener) taskListeners.add((Consumer) consumer);
  }

  /**
   * Handles a task.
   */
  @SuppressWarnings("unchecked")
  void handleTask(GroupTask task) {
    taskListeners.accept(task);
  }

  @Override
  public GroupConnection connection() {
    return connection;
  }

  /**
   * Registers a callback to be called when this member is elected leader.
   * <p>
   * The provided {@link Consumer} will be called when the local member is elected leader.
   * The term for which the member was elected leader will be provided to the callback.
   * <pre>
   *   {@code
   *   group.join().thenAccept(member -> {
   *     member.onElection(term -> {
   *       System.out.println("Elected leader for term " + term);
   *     });
   *   });
   *   }
   * </pre>
   * Leader election is performed using a fair algorithm. However, once the member is elected
   * leader, it is not guaranteed to remain the leader of the group until failure. In the
   * event that the local group instance is partitioned from the rest of the cluster, the
   * member may be removed from leadership and another member of the group may be elected.
   * Users should use the provided {@code term} for fencing when interacting with external
   * resources.
   *
   * @param callback The callback to call.
   * @return The leader election listener.
   */
  public Listener<Long> onElection(Consumer<Long> callback) {
    return group.election().onElection(memberId, callback);
  }

  /**
   * Resigns from leadership.
   * <p>
   * Resigning as the leader of the group will result in the local member being placed at the
   * tail of the leader election queue. Once the member has resigned, the {@link GroupElection#term()}
   * will be incremented and a new leader will be elected from the leader queue. If the local member
   * is the only member of the group, it will immediately be reassigned as the leader for the new
   * term.
   * <pre>
   *   {@code
   *   member.onElection(term -> {
   *     // I don't want to be the leader!
   *     member.resign();
   *   });
   *   }
   * </pre>
   *
   * @return A completable future to be completed once the member has resigned.
   */
  public CompletableFuture<Void> resign() {
    return group.submit(new GroupCommands.Resign(memberId));
  }

  /**
   * Leaves the membership group.
   * <p>
   * When this member leaves the membership group, the membership lists of this and all other instances
   * in the group are guaranteed to be updated <em>before</em> the {@link CompletableFuture} returned by
   * this method is completed. Once this instance has left the group, the returned future will be completed.
   * <p>
   * This method returns a {@link CompletableFuture} which can be used to block until the operation completes
   * or to be notified in a separate thread once the operation completes. To block until the operation completes,
   * use the {@link CompletableFuture#join()} method to block the calling thread:
   * <pre>
   *   {@code
   *   member.leave().join();
   *   }
   * </pre>
   * Alternatively, to execute the operation asynchronous and be notified once the lock is acquired in a different
   * thread, use one of the many completable future callbacks:
   * <pre>
   *   {@code
   *   member.leave().thenRun(() -> System.out.println("Left the group!")));
   *   }
   * </pre>
   *
   * @return A completable future to be completed once the member has left.
   */
  public CompletableFuture<Void> leave() {
    return group.submit(new GroupCommands.Leave(memberId)).whenComplete((result, error) -> {
      group.members.remove(memberId);
    });
  }

  /**
   * Listener holder.
   */
  @SuppressWarnings("unchecked")
  private class MessageListenerHolder implements Listener {
    private final Consumer consumer;

    private MessageListenerHolder(Consumer consumer) {
      this.consumer = consumer;
    }

    @Override
    public void accept(Object message) {
      consumer.accept(message);
    }

    @Override
    public void close() {
      messageListeners.remove(this);
    }
  }

}
