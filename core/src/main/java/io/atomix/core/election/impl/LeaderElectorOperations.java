/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.election.impl;

import com.google.common.base.MoreObjects;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link io.atomix.core.election.LeaderElector} operations.
 * <p>
 * WARNING: Do not refactor enum values. Only add to them.
 * Changing values risk breaking the ability to backup/restore/upgrade clusters.
 */
public enum LeaderElectorOperations implements OperationId {
  ADD_LISTENER(OperationType.COMMAND),
  REMOVE_LISTENER(OperationType.COMMAND),
  RUN(OperationType.COMMAND),
  WITHDRAW(OperationType.COMMAND),
  ANOINT(OperationType.COMMAND),
  PROMOTE(OperationType.COMMAND),
  EVICT(OperationType.COMMAND),
  GET_LEADERSHIP(OperationType.QUERY),
  GET_ALL_LEADERSHIPS(OperationType.QUERY),
  GET_ELECTED_TOPICS(OperationType.QUERY);

  private final OperationType type;

  LeaderElectorOperations(OperationType type) {
    this.type = type;
  }

  @Override
  public String id() {
    return name();
  }

  @Override
  public OperationType type() {
    return type;
  }

  public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(Run.class)
      .register(Withdraw.class)
      .register(Anoint.class)
      .register(Promote.class)
      .register(Evict.class)
      .register(GetLeadership.class)
      .register(GetElectedTopics.class)
      .build(LeaderElectorOperations.class.getSimpleName());

  /**
   * Abstract election query.
   */
  @SuppressWarnings("serial")
  public abstract static class ElectionOperation {
  }

  /**
   * Abstract election topic query.
   */
  @SuppressWarnings("serial")
  public abstract static class TopicOperation extends ElectionOperation {
    String topic;

    public TopicOperation() {
    }

    public TopicOperation(String topic) {
      this.topic = checkNotNull(topic);
    }

    /**
     * Returns the topic.
     *
     * @return topic
     */
    public String topic() {
      return topic;
    }
  }

  /**
   * GetLeader query.
   */
  @SuppressWarnings("serial")
  public static class GetLeadership extends TopicOperation {

    public GetLeadership() {
    }

    public GetLeadership(String topic) {
      super(topic);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("topic", topic)
          .toString();
    }
  }

  /**
   * GetElectedTopics query.
   */
  @SuppressWarnings("serial")
  public static class GetElectedTopics extends ElectionOperation {
    private byte[] id;

    public GetElectedTopics() {
    }

    public GetElectedTopics(byte[] id) {
      checkArgument(id != null, "id cannot be null");
      this.id = id;
    }

    /**
     * Returns the id to check.
     *
     * @return The id to check.
     */
    public byte[] id() {
      return id;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("id", id)
          .toString();
    }
  }

  /**
   * Command for administratively changing the leadership state for a node.
   */
  @SuppressWarnings("serial")
  public abstract static class ElectionChangeOperation extends ElectionOperation {
    private String topic;
    private byte[] id;

    ElectionChangeOperation() {
      this.topic = null;
      this.id = null;
    }

    ElectionChangeOperation(String topic, byte[] id) {
      this.topic = topic;
      this.id = id;
    }

    /**
     * Returns the topic.
     *
     * @return The topic
     */
    public String topic() {
      return topic;
    }

    /**
     * Returns the id to make leader.
     *
     * @return The id
     */
    public byte[] id() {
      return id;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("topic", topic)
          .add("id", id)
          .toString();
    }
  }

  /**
   * Enter and run for leadership.
   */
  @SuppressWarnings("serial")
  public static class Run extends ElectionChangeOperation {
    public Run() {
    }

    public Run(String topic, byte[] id) {
      super(topic, id);
    }
  }

  /**
   * Withdraw from a leadership contest.
   */
  @SuppressWarnings("serial")
  public static class Withdraw extends ElectionChangeOperation {
    public Withdraw() {
    }

    public Withdraw(String topic, byte[] id) {
      super(topic, id);
    }
  }

  /**
   * Command for administratively anoint a node as leader.
   */
  @SuppressWarnings("serial")
  public static class Anoint extends ElectionChangeOperation {

    private Anoint() {
    }

    public Anoint(String topic, byte[] id) {
      super(topic, id);
    }
  }

  /**
   * Command for administratively promote a node as top candidate.
   */
  @SuppressWarnings("serial")
  public static class Promote extends ElectionChangeOperation {

    private Promote() {
    }

    public Promote(String topic, byte[] id) {
      super(topic, id);
    }
  }

  /**
   * Command for administratively evicting a node from all leadership topics.
   */
  @SuppressWarnings("serial")
  public static class Evict extends ElectionOperation {
    private byte[] id;

    public Evict() {
    }

    public Evict(byte[] id) {
      this.id = id;
    }

    /**
     * Returns the node identifier.
     *
     * @return The id
     */
    public byte[] id() {
      return id;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("id", id)
          .toString();
    }
  }
}