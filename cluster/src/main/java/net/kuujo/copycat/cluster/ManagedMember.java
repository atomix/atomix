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

import net.kuujo.copycat.util.ExecutionContext;

import java.util.Random;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ManagedMember implements Member {
  protected final MemberInfo info;
  protected Type type;
  protected Status status = Status.DEAD;
  protected Session session;
  protected final ExecutionContext context;

  protected ManagedMember(MemberInfo info, Type type, ExecutionContext context) {
    this.info = info;
    this.type = type;
    this.context = context;
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
    return info.id();
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public MemberInfo info() {
    return info;
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Member && ((Member) object).id() == info.id();
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), info.id());
  }

  /**
   * Member builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends ManagedMember> implements Member.Builder<T, U> {
    protected int id = new Random().nextInt();

    @Override
    @SuppressWarnings("unchecked")
    public T withId(int id) {
      this.id = id;
      return (T) this;
    }
  }

}
