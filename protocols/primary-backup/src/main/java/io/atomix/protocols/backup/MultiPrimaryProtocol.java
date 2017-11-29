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
package io.atomix.protocols.backup;

import io.atomix.primitive.Consistency;
import io.atomix.primitive.PrimitiveProtocol;
import io.atomix.primitive.Replication;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Multi-primary protocol.
 */
public class MultiPrimaryProtocol implements PrimitiveProtocol {
  public static final Type TYPE = new Type() {};

  /**
   * Returns a new multi-primary protocol builder.
   *
   * @return a new multi-primary protocol builder
   */
  public static Builder builder() {
    return builder(null);
  }

  /**
   * Returns a new multi-primary protocol builder for the given group.
   *
   * @param group the partition group
   * @return a new multi-primary protocol builder for the given group
   */
  public static Builder builder(String group) {
    return new Builder(group);
  }

  private final String group;
  private final Consistency consistency;
  private final Replication replication;
  private final int backups;

  protected MultiPrimaryProtocol(String group, Consistency consistency, Replication replication, int backups) {
    this.group = group;
    this.consistency = consistency;
    this.replication = replication;
    this.backups = backups;
  }

  @Override
  public Type type() {
    return TYPE;
  }

  @Override
  public String group() {
    return group;
  }

  /**
   * Returns the protocol consistency model.
   *
   * @return the protocol consistency model
   */
  public Consistency consistency() {
    return consistency;
  }

  /**
   * Returns the protocol replications strategy.
   *
   * @return the protocol replication strategy
   */
  public Replication replication() {
    return replication;
  }

  /**
   * Returns the number of backups.
   *
   * @return the number of backups
   */
  public int backups() {
    return backups;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("type", type())
        .add("group", group())
        .add("consistency", consistency())
        .add("replication", replication())
        .add("backups", backups())
        .toString();
  }

  /**
   * Multi-primary protocol builder.
   */
  public static class Builder extends PrimitiveProtocol.Builder {
    private Consistency consistency = Consistency.SEQUENTIAL;
    private Replication replication = Replication.SYNCHRONOUS;
    private int numBackups;

    protected Builder(String group) {
      super(group);
    }

    /**
     * Sets the protocol consistency model.
     *
     * @param consistency the protocol consistency model
     * @return the protocol builder
     */
    public Builder withConsistency(Consistency consistency) {
      this.consistency = checkNotNull(consistency, "consistency cannot be null");
      return this;
    }

    /**
     * Sets the protocol replication strategy.
     *
     * @param replication the protocol replication strategy
     * @return the protocol builder
     */
    public Builder withReplication(Replication replication) {
      this.replication = checkNotNull(replication, "replication cannot be null");
      return this;
    }

    /**
     * Sets the number of backups.
     *
     * @param numBackups the number of backups
     * @return the protocol builder
     */
    public Builder withBackups(int numBackups) {
      checkArgument(numBackups >= 0, "numBackups must be positive");
      this.numBackups = numBackups;
      return this;
    }

    @Override
    public MultiPrimaryProtocol build() {
      return new MultiPrimaryProtocol(group, consistency, replication, numBackups);
    }
  }
}
