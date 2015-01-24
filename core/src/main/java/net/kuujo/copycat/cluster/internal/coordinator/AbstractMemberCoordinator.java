/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.cluster.internal.coordinator;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.internal.MemberInfo;
import net.kuujo.copycat.util.internal.Assert;

import java.util.concurrent.CompletableFuture;

/**
 * Base member coordinator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractMemberCoordinator implements MemberCoordinator {
  private final MemberInfo info;
  private boolean open;

  protected AbstractMemberCoordinator(MemberInfo info) {
    this.info = Assert.isNotNull(info, "info");
  }

  MemberInfo info() {
    return info;
  }

  @Override
  public String uri() {
    return info.uri();
  }

  @Override
  public Member.Type type() {
    return info.type();
  }

  @Override
  public Member.State state() {
    return info.state();
  }

  @Override
  public CompletableFuture<MemberCoordinator> open() {
    open = true;
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
