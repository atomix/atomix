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
import net.kuujo.copycat.raft.RaftMember;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.Managed;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ManagedMember<T extends Member> implements Member, Managed<T> {
  protected final RaftMember member;
  protected final ResourceContext context;

  public ManagedMember(RaftMember member, ResourceContext context) {
    this.member = member;
    this.context = context;
  }

  @Override
  public String uri() {
    return member.uri();
  }

  @Override
  public Type type() {
    return Member.Type.lookup(member.type());
  }

  @Override
  public Status status() {
    return Member.Status.lookup(member.status());
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Member && ((Member) object).uri().equals(uri());
  }

  abstract CompletableFuture<ByteBuffer> sendInternal(String topic, ByteBuffer request);

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + uri().hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[uri=%s, type=%s, status=%s]", getClass().getSimpleName(), member.uri(), member.type(), member.status());
  }

}
