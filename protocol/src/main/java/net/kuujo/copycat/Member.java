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
package net.kuujo.copycat;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;

/**
 * Configuration for connecting to a member of the Raft cluster.
 * <p>
 * Instances of this class are used to provide configurations for connecting to Raft servers. Each member in a Raft
 * cluster must be defined by a unique {@link #id()} which is consistent across all clients and servers in the cluster.
 * The provided {@link #host()} and {@link #port()} will be used by {@link Raft} instances to
 * connect to servers in the cluster.
 * <p>
 * Members are immutable and therefore threadsafe. To create a {@link Member}, create a
 * {@link Member.Builder} via the static {@link Member#builder()} method.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Member implements AlleycatSerializable {

  /**
   * Returns a new member builder.
   * <p>
   * The returned {@link Member.Builder} is pooled internally via a {@link net.kuujo.copycat.BuilderPool}.
   * Once the builder's {@link Member.Builder#build()} method is called, the builder will be
   * released back to the internal pool and recycled on the next call to this method.
   *
   * @return A new member builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private int id;
  private String host;
  private int port;

  public Member() {
  }

  public Member(int id) {
    this.id = id;
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the member host.
   *
   * @return The member host.
   */
  public String host() {
    return host;
  }

  /**
   * Returns the member port.
   *
   * @return The member port.
   */
  public int port() {
    return port;
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    buffer.writeInt(id)
      .writeString(host)
      .writeInt(port);
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    id = buffer.readInt();
    host = buffer.readString();
    port = buffer.readInt();
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Member) {
      Member member = (Member) object;
      return member.id == id && member.host.equals(host) && member.port == port;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id;
    hashCode = 37 * hashCode + host.hashCode();
    hashCode = 37 * hashCode + port;
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d, host=%s, port=%d]", getClass().getSimpleName(), id, host, port);
  }

  /**
   * Builds
   */
  public static class Builder extends net.kuujo.copycat.Builder<Member> {
    private Member member = new Member();

    private Builder() {
    }

    @Override
    protected void reset() {
      super.reset();
      this.member = new Member();
    }

    @Override
    protected void reset(Member member) {
      this.member = member;
    }

    /**
     * Sets the member ID.
     *
     * @param id The member ID.
     * @return The member builder.
     */
    public Builder withId(int id) {
      member.id = id;
      return this;
    }

    /**
     * Sets the member host.
     *
     * @param host The member host.
     * @return The member builder.
     */
    public Builder withHost(String host) {
      member.host = host;
      return this;
    }

    /**
     * Sets the member port.
     *
     * @param port The member port.
     * @return The member builder.
     */
    public Builder withPort(int port) {
      member.port = port;
      return this;
    }

    @Override
    public Member build() {
      close();
      return member;
    }
  }

}
