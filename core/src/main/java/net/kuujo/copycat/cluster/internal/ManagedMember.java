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
package net.kuujo.copycat.cluster.internal;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.raft.RaftContext;
import net.kuujo.copycat.raft.RaftMember;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.Managed;

import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ManagedMember<T extends Member> implements Member, Managed<T>, Observer {
  protected final ResourceContext context;
  private final int id;
  private String address;
  private Type type;
  private Status status;

  public ManagedMember(int id, ResourceContext context) {
    if (id <= 0)
      throw new IllegalArgumentException("id cannot be negative");
    if (context == null)
      throw new NullPointerException("context cannot be null");
    this.id = id;
    this.context = context;
  }

  @Override
  public void update(Observable o, Object arg) {
    RaftContext raft = (RaftContext) o;
    RaftMember member = raft.getMember(id);
    if (member != null) {
      address = member.get("address");
      type = Type.lookup(member.type());
      status = Status.lookup(member.status());
    }
  }

  @Override
  public int id() {
    return id;
  }

  /**
   * Checks that the member is open.
   */
  private void checkOpen() {
    if (!isOpen())
      throw new IllegalStateException("member not open");
  }

  @Override
  public String address() {
    checkOpen();
    return address;
  }

  @Override
  public Type type() {
    checkOpen();
    return type;
  }

  @Override
  public Status status() {
    checkOpen();
    return status;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Member && ((Member) object).address().equals(address());
  }

  /**
   * Sends an internal message.
   */
  abstract CompletableFuture<Buffer> sendInternal(String topic, Buffer request);

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> open() {
    context.raft().addObserver(this);
    return CompletableFuture.completedFuture((T) this);
  }

  @Override
  public CompletableFuture<Void> close() {
    context.raft().deleteObserver(this);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s, address=%s, type=%s, status=%s]", getClass().getSimpleName(), id, address, type, status);
  }

}
