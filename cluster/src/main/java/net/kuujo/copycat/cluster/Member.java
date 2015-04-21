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
 * limitations under the License.
 */
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.io.serializer.CopycatSerializer;
import net.kuujo.copycat.util.concurrent.ExecutionContext;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Member {

  /**
   * Cluster member type.<p>
   *
   * The member type indicates how cluster members behave in terms of joining and leaving the cluster and how the
   * members participate in log replication. {@link Type#ACTIVE} members are full voting members of the cluster that
   * participate in Copycat's consensus protocol. {@link Type#PASSIVE} members may join and leave the cluster at will
   * without impacting the availability of a resource and receive only committed log entries via a gossip protocol.
   */
  public static enum Type {

    /**
     * Indicates that the member is a remote client of the cluster.
     */
    REMOTE,

    /**
     * Indicates that the member is a passive, non-voting member of the cluster.
     */
    PASSIVE,

    /**
     * Indicates that the member is an active voting member of the cluster.
     */
    ACTIVE
  }

  /**
   * Cluster member status.<p>
   *
   * The member status indicates how a given member is perceived by the local node. Members can be in one of three states
   * at any given time, {@link net.kuujo.copycat.cluster.Member.Status#ALIVE}, {@link net.kuujo.copycat.cluster.Member.Status#SUSPICIOUS}, and {@link net.kuujo.copycat.cluster.Member.Status#DEAD}. Member states are changed
   * according to the local node's ability to communicate with a given member. All members begin with an
   * {@link net.kuujo.copycat.cluster.Member.Status#ALIVE} status upon joining the cluster. If the member appears to be unreachable, its status will be
   * changed to {@link net.kuujo.copycat.cluster.Member.Status#SUSPICIOUS}, indicating that it may have left the cluster or died. Once enough other nodes
   * in the cluster agree that the suspicious member appears to be dead, the status will be changed to {@link net.kuujo.copycat.cluster.Member.Status#DEAD}
   * and the member will ultimately be removed from the cluster configuration.
   */
  public static enum Status {

    /**
     * Indicates that the member is considered to be dead.
     */
    DEAD,

    /**
     * Indicates that the member is suspicious and is unreachable by at least one other member.
     */
    SUSPICIOUS,

    /**
     * Indicates that the member is alive and reachable.
     */
    ALIVE
  }

  /**
   * Member info.
   */
  public static class Info implements CopycatSerializable {
    private static final int FAILURE_LIMIT = 3;
    private Type type;
    private Status status;
    private long changed;
    private int id;
    String address;
    private long version = 1;
    private Set<Integer> failures = new HashSet<>();

    public Info() {
    }

    public Info(int id, Type type) {
      this(id, type, Status.ALIVE);
    }

    public Info(int id, Type type, Status status) {
      this(id, type, status, 1);
    }

    public Info(int id, Type type, Status status, long version) {
      this.id = id;
      this.type = type;
      this.status = status;
      this.version = version;
    }

    /**
     * Returns the member identifier.
     *
     * @return The unique member identifier.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the member address.
     *
     * @return The member address.
     */
    public String address() {
      return address;
    }

    /**
     * Returns the member type.
     *
     * @return The member type.
     */
    public Type type() {
      return type;
    }

    /**
     * Returns the member state.
     *
     * @return The member state.
     */
    public Status status() {
      return status;
    }

    /**
     * Returns the last time the member state changed.
     *
     * @return The last time the member state changed.
     */
    public long changed() {
      return changed;
    }

    /**
     * Returns the member version.
     *
     * @return The member version.
     */
    public long version() {
      return version;
    }

    /**
     * Sets the member version.
     *
     * @param version The member version.
     * @return The member info.
     */
    public Info version(long version) {
      this.version = version;
      return this;
    }

    /**
     * Marks a successful gossip with the member.
     *
     * @return The member info.
     */
    public Info succeed() {
      if (type == Member.Type.PASSIVE && status != Member.Status.ALIVE) {
        failures.clear();
        status = Member.Status.ALIVE;
        changed = System.currentTimeMillis();
      }
      return this;
    }

    /**
     * Marks a failure in the member.
     *
     * @param id The id recording the failure.
     * @return The member info.
     */
    public Info fail(int id) {
      // If the member is a passive member, add the failure to the failures set and change the state. If the current
      // state is ACTIVE then change the state to SUSPICIOUS. If the current state is SUSPICIOUS and the number of
      // failures from *unique* nodes is equal to or greater than the failure limit then change the state to DEAD.
      if (type == Member.Type.PASSIVE) {
        failures.add(id);
        if (status == Member.Status.ALIVE) {
          status = Member.Status.SUSPICIOUS;
          changed = System.currentTimeMillis();
        } else if (status == Member.Status.SUSPICIOUS) {
          if (failures.size() >= FAILURE_LIMIT) {
            status = Member.Status.DEAD;
            changed = System.currentTimeMillis();
          }
        }
      }
      return this;
    }

    /**
     * Updates the member info.
     *
     * @param info The member info to update.
     */
    public void update(Info info) {
      // If the given version is greater than the current version then update the member state.
      if (info.version > this.version) {
        this.version = info.version;

        // Any time the version is incremented, clear failures for the previous version.
        this.failures.clear();

        // Only passive member types can experience state changes. Active members are always alive.
        if (this.type == Member.Type.PASSIVE) {
          // If the state changed then update the state and set the last changed time. This can be used to clean
          // up state related to old members after some period of time.
          if (this.status != info.status) {
            changed = System.currentTimeMillis();
          }
          this.status = info.status;
        }
      } else if (info.version == this.version) {
        if (info.status == Member.Status.SUSPICIOUS) {
          // If the given version is the same as the current version then update failures. If the member has experienced
          // FAILURE_LIMIT failures then transition the member's state to DEAD.
          this.failures.addAll(info.failures);
          if (this.failures.size() >= FAILURE_LIMIT) {
            this.status = Member.Status.DEAD;
            changed = System.currentTimeMillis();
          }
        } else if (info.status == Member.Status.DEAD) {
          this.status = Member.Status.DEAD;
          changed = System.currentTimeMillis();
        }
      }
    }

    @Override
    public void writeObject(Buffer buffer) {
      buffer.writeByte(type.ordinal())
        .writeByte(status.ordinal())
        .writeInt(id)
        .writeInt(address.getBytes().length)
        .write(address.getBytes())
        .writeLong(version);
      buffer.writeInt(failures.size());
      failures.forEach(buffer::writeInt);
    }

    @Override
    public void readObject(Buffer buffer) {
      type = Type.values()[buffer.readByte()];
      status = Status.values()[buffer.readByte()];
      id = buffer.readInt();
      byte[] addressBytes = new byte[buffer.readInt()];
      buffer.read(addressBytes);
      address = new String(addressBytes);
      version = buffer.readLong();
      int numFailures = buffer.readInt();
      failures = new HashSet<>(numFailures);
      for (int i = 0; i < numFailures; i++) {
        failures.add(buffer.readInt());
      }
    }

    @Override
    public boolean equals(Object object) {
      if (object instanceof Info) {
        Info member = (Info) object;
        return member.id == id
          && member.type == type
          && member.status == status
          && member.version == version;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, type, status, version);
    }

    @Override
    public String toString() {
      return String.format("Member.Info[id=%s, type=%s, state=%s, version=%d]", id, type, status, version);
    }
  }

  protected final Info info;
  protected final CopycatSerializer serializer;
  protected final ExecutionContext context;

  protected Member(Info info, CopycatSerializer serializer, ExecutionContext context) {
    this.info = info;
    this.serializer = serializer;
    this.context = context;
  }

  /**
   * Returns the current execution context.
   */
  protected ExecutionContext getContext() {
    ExecutionContext context = ExecutionContext.currentContext();
    return context != null ? context : this.context;
  }

  /**
   * Returns the unique member ID.
   *
   * @return The unique member ID.
   */
  public int id() {
    return info.id;
  }

  /**
   * Returns the member address.
   *
   * @return The member address.
   */
  public abstract String address();

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  public Type type() {
    return info.type;
  }

  /**
   * Returns the member status.
   *
   * @return The member status.
   */
  public Status status() {
    return info.status;
  }

  /**
   * Sends a message to the member.<p>
   *
   * Messages are sent using a topic based messaging system over the configured cluster protocol. If no handler is
   * registered for the given topic on the given member, the returned {@link java.util.concurrent.CompletableFuture}
   * will be failed. If the member successfully receives the message and responds, the returned
   * {@link java.util.concurrent.CompletableFuture} will be completed with the member's response.
   *
   * @param topic The topic on which to send the message.
   * @param message The message to send. Messages will be serialized using the configured resource serializer.
   * @param <T> The message type.
   * @param <U> The response type.
   * @return A completable future to be completed with the message response.
   */
  public abstract <T, U> CompletableFuture<U> send(String topic, T message);

  /**
   * Executes a task on the member.
   *
   * @param task The task to execute.
   * @return A completable future to be completed once the task has been executed.
   */
  public abstract CompletableFuture<Void> execute(Task<Void> task);

  /**
   * Submits a task to the member, returning a future to be completed with the remote task result.
   *
   * @param task The task to submit to the member.
   * @param <T> The task return type.
   * @return A completable future to be completed with the task result.
   */
  public abstract <T> CompletableFuture<T> submit(Task<T> task);

  @Override
  public boolean equals(Object object) {
    return object instanceof net.kuujo.copycat.cluster.Member && ((net.kuujo.copycat.cluster.Member) object).address().equals(address());
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, address=%s, type=%s, status=%s]", getClass().getSimpleName(), info.id, address(), info.type, info.status);
  }

  /**
   * Member builder.
   */
  public static abstract class Builder<T extends Builder<T>> {
    protected int id;
    protected Type type;
    protected CopycatSerializer serializer;
    protected ExecutionContext context;

    /**
     * Sets the member identifier.
     *
     * @param id The member identifier.
     * @return The member builder.
     */
    @SuppressWarnings("unchecked")
    public T withId(int id) {
      this.id = id;
      return (T) this;
    }

    /**
     * Sets the member type.
     *
     * @param type The member type.
     * @return The member builder.
     */
    @SuppressWarnings("unchecked")
    public T withType(Type type) {
      if (type == null)
        throw new NullPointerException("type cannot be null");
      this.type = type;
      return (T) this;
    }

    /**
     * Sets the cluster serializer.
     *
     * @param serializer The cluster serializer.
     * @return The cluster builder.
     */
    @SuppressWarnings("unchecked")
    public T withSerializer(CopycatSerializer serializer) {
      if (serializer == null)
        throw new NullPointerException("serializer cannot be null");
      this.serializer = serializer;
      return (T) this;
    }

    /**
     * Sets the cluster execution context.
     *
     * @param context The cluster execution context.
     * @return The cluster builder.
     */
    @SuppressWarnings("unchecked")
    public T withExecutionContext(ExecutionContext context) {
      if (context == null)
        throw new NullPointerException("context cannot be null");
      this.context = context;
      return (T) this;
    }

    /**
     * Builds the member.
     *
     * @return The built member.
     */
    public abstract Member build();
  }

}
