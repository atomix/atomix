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

import io.atomix.core.election.Leader;
import io.atomix.core.election.Leadership;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.utils.ArraySizeHashPrinter;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;

/**
 * {@link LeaderElectionProxy} resource state machine operations.
 */
public enum LeaderElectionOperations implements OperationId {
  ADD_LISTENER("addListener", OperationType.COMMAND),
  REMOVE_LISTENER("removeListener", OperationType.COMMAND),
  RUN("run", OperationType.COMMAND),
  WITHDRAW("withdraw", OperationType.COMMAND),
  ANOINT("anoint", OperationType.COMMAND),
  PROMOTE("promote", OperationType.COMMAND),
  EVICT("evict", OperationType.COMMAND),
  GET_LEADERSHIP("getLeadership", OperationType.QUERY);

  private final String id;
  private final OperationType type;

  LeaderElectionOperations(String id, OperationType type) {
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

  public static final KryoNamespace NAMESPACE = KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .nextId(KryoNamespaces.BEGIN_USER_CUSTOM_ID)
      .register(Leadership.class)
      .register(Leader.class)
      .register(Run.class)
      .register(Withdraw.class)
      .register(Anoint.class)
      .register(Promote.class)
      .register(Evict.class)
      .build(LeaderElectionOperations.class.getSimpleName());

  /**
   * Abstract election operation.
   */
  @SuppressWarnings("serial")
  public abstract static class ElectionOperation {
  }

  /**
   * Election operation that uses an instance identifier.
   */
  public abstract static class ElectionChangeOperation extends ElectionOperation {
    private byte[] id;

    public ElectionChangeOperation() {
    }

    public ElectionChangeOperation(byte[] id) {
      this.id = id;
    }

    /**
     * Returns the candidate identifier.
     *
     * @return the candidate identifier
     */
    public byte[] id() {
      return id;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(getClass())
          .add("id", ArraySizeHashPrinter.of(id))
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

    public Run(byte[] id) {
      super(id);
    }
  }

  /**
   * Command for withdrawing a candidate from an election.
   */
  @SuppressWarnings("serial")
  public static class Withdraw extends ElectionChangeOperation {
    private Withdraw() {
    }

    public Withdraw(byte[] id) {
      super(id);
    }
  }

  /**
   * Command for administratively anoint a node as leader.
   */
  @SuppressWarnings("serial")
  public static class Anoint extends ElectionChangeOperation {
    private Anoint() {
    }

    public Anoint(byte[] id) {
      super(id);
    }
  }

  /**
   * Command for administratively promote a node as top candidate.
   */
  @SuppressWarnings("serial")
  public static class Promote extends ElectionChangeOperation {
    private Promote() {
    }

    public Promote(byte[] id) {
      super(id);
    }
  }

  /**
   * Command for administratively evicting a node from all leadership topics.
   */
  @SuppressWarnings("serial")
  public static class Evict extends ElectionChangeOperation {
    public Evict() {
    }

    public Evict(byte[] id) {
      super(id);
    }
  }
}