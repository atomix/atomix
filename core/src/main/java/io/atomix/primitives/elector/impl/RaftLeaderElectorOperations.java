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
package io.atomix.primitives.elector.impl;

import com.google.common.base.MoreObjects;
import io.atomix.cluster.NodeId;
import io.atomix.protocols.raft.operation.OperationId;
import io.atomix.protocols.raft.operation.OperationType;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link RaftLeaderElector} resource state machine operations.
 */
public enum RaftLeaderElectorOperations implements OperationId {
  ADD_LISTENER("addListener", OperationType.COMMAND),
  REMOVE_LISTENER("removeListener", OperationType.COMMAND),
  RUN("run", OperationType.COMMAND),
  WITHDRAW("withdraw", OperationType.COMMAND),
  ANOINT("anoint", OperationType.COMMAND),
  PROMOTE("promote", OperationType.COMMAND),
  EVICT("evict", OperationType.COMMAND),
  GET_LEADERSHIP("getLeadership", OperationType.QUERY),
  GET_ALL_LEADERSHIPS("getAllLeaderships", OperationType.QUERY),
  GET_ELECTED_TOPICS("getElectedTopics", OperationType.QUERY);

  private final String id;
  private final OperationType type;

  RaftLeaderElectorOperations(String id, OperationType type) {
    this.id = id;
    this.type = type;
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public OperationType type() {
    return type;
  }

  public static final KryoNamespace NAMESPACE = KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(NodeId.class)
      .register(Run.class)
      .register(Withdraw.class)
      .register(Anoint.class)
      .register(Promote.class)
      .register(Evict.class)
      .register(GetLeadership.class)
      .register(GetElectedTopics.class)
      .build("AtomixLeaderElectorOperations");

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
    private NodeId nodeId;

    public GetElectedTopics() {
    }

    public GetElectedTopics(NodeId nodeId) {
      checkArgument(nodeId != null, "nodeId cannot be null");
      this.nodeId = nodeId;
    }

    /**
     * Returns the nodeId to check.
     *
     * @return The nodeId to check.
     */
    public NodeId nodeId() {
      return nodeId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("nodeId", nodeId)
          .toString();
    }
  }

  /**
   * Enter and run for leadership.
   */
  @SuppressWarnings("serial")
  public static class Run extends ElectionOperation {
    private String topic;
    private NodeId nodeId;

    public Run() {
    }

    public Run(String topic, NodeId nodeId) {
      this.topic = topic;
      this.nodeId = nodeId;
    }

    /**
     * Returns the topic.
     *
     * @return topic
     */
    public String topic() {
      return topic;
    }

    /**
     * Returns the nodeId.
     *
     * @return the nodeId
     */
    public NodeId nodeId() {
      return nodeId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("topic", topic)
          .add("nodeId", nodeId)
          .toString();
    }
  }

  /**
   * Withdraw from a leadership contest.
   */
  @SuppressWarnings("serial")
  public static class Withdraw extends ElectionOperation {
    private String topic;

    public Withdraw() {
    }

    public Withdraw(String topic) {
      this.topic = topic;
    }

    /**
     * Returns the topic.
     *
     * @return The topic
     */
    public String topic() {
      return topic;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("topic", topic)
          .toString();
    }
  }

  /**
   * Command for administratively changing the leadership state for a node.
   */
  @SuppressWarnings("serial")
  public abstract static class ElectionChangeOperation extends ElectionOperation {
    private String topic;
    private NodeId nodeId;

    ElectionChangeOperation() {
      topic = null;
      nodeId = null;
    }

    public ElectionChangeOperation(String topic, NodeId nodeId) {
      this.topic = topic;
      this.nodeId = nodeId;
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
     * Returns the nodeId to make leader.
     *
     * @return The nodeId
     */
    public NodeId nodeId() {
      return nodeId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("topic", topic)
          .add("nodeId", nodeId)
          .toString();
    }
  }

  /**
   * Command for administratively anoint a node as leader.
   */
  @SuppressWarnings("serial")
  public static class Anoint extends ElectionChangeOperation {

    private Anoint() {
    }

    public Anoint(String topic, NodeId nodeId) {
      super(topic, nodeId);
    }
  }

  /**
   * Command for administratively promote a node as top candidate.
   */
  @SuppressWarnings("serial")
  public static class Promote extends ElectionChangeOperation {

    private Promote() {
    }

    public Promote(String topic, NodeId nodeId) {
      super(topic, nodeId);
    }
  }

  /**
   * Command for administratively evicting a node from all leadership topics.
   */
  @SuppressWarnings("serial")
  public static class Evict extends ElectionOperation {
    private NodeId nodeId;

    public Evict() {
    }

    public Evict(NodeId nodeId) {
      this.nodeId = nodeId;
    }

    /**
     * Returns the node identifier.
     *
     * @return The nodeId
     */
    public NodeId nodeId() {
      return nodeId;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("nodeId", nodeId)
          .toString();
    }
  }
}