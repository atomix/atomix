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
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.util.ExecutionContext;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractMember implements Member {

  /**
   * Member info.
   */
  public static class Info implements Writable {
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
      if (type == Type.PASSIVE && status != Status.ALIVE) {
        failures.clear();
        status = Status.ALIVE;
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
      if (type == Type.PASSIVE) {
        failures.add(id);
        if (status == Status.ALIVE) {
          status = Status.SUSPICIOUS;
          changed = System.currentTimeMillis();
        } else if (status == Status.SUSPICIOUS) {
          if (failures.size() >= FAILURE_LIMIT) {
            status = Status.DEAD;
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
        if (this.type == Type.PASSIVE) {
          // If the state changed then update the state and set the last changed time. This can be used to clean
          // up state related to old members after some period of time.
          if (this.status != info.status) {
            changed = System.currentTimeMillis();
          }
          this.status = info.status;
        }
      } else if (info.version == this.version) {
        if (info.status == Status.SUSPICIOUS) {
          // If the given version is the same as the current version then update failures. If the member has experienced
          // FAILURE_LIMIT failures then transition the member's state to DEAD.
          this.failures.addAll(info.failures);
          if (this.failures.size() >= FAILURE_LIMIT) {
            this.status = Status.DEAD;
            changed = System.currentTimeMillis();
          }
        } else if (info.status == Status.DEAD) {
          this.status = Status.DEAD;
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
  protected final Serializer serializer;
  protected final ExecutionContext context;

  protected AbstractMember(Info info, Serializer serializer, ExecutionContext context) {
    this.info = info;
    this.serializer = serializer;
    this.context = context;
  }

  /**
   * Returns the member info.
   *
   * @return The member info.
   */
  Info info() {
    return info;
  }

  /**
   * Returns the current execution context.
   */
  protected ExecutionContext getContext() {
    ExecutionContext context = ExecutionContext.currentContext();
    return context != null ? context : this.context;
  }

  @Override
  public int id() {
    return info.id;
  }

  @Override
  public Type type() {
    return info.type;
  }

  @Override
  public Status status() {
    return info.status;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Member && ((Member) object).id() == info.id;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, type=%s, status=%s]", getClass().getSimpleName(), info.id, info.type, info.status);
  }

  /**
   * Member builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends AbstractMember> implements Member.Builder<T, U> {
    protected int id;
    protected Type type;
    protected Serializer serializer;

    @Override
    @SuppressWarnings("unchecked")
    public T withId(int id) {
      this.id = id;
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withType(Type type) {
      if (type == null)
        throw new NullPointerException("type cannot be null");
      this.type = type;
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withSerializer(Serializer serializer) {
      if (serializer == null)
        throw new NullPointerException("serializer cannot be null");
      this.serializer = serializer;
      return (T) this;
    }
  }

}
