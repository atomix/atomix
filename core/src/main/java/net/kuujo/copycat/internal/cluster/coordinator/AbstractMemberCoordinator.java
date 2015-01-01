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
package net.kuujo.copycat.internal.cluster.coordinator;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.cluster.coordinator.MemberCoordinator;

import java.util.concurrent.CompletableFuture;

/**
 * Base member coordinator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractMemberCoordinator implements MemberCoordinator {
  private final String uri;
  private final Member.Type type;
  private Member.State state;
  private boolean open;

  protected AbstractMemberCoordinator(String uri, Member.Type type, Member.State state) {
    this.uri = uri;
    this.type = type;
    this.state = state;
  }

  @Override
  public String uri() {
    return uri;
  }

  @Override
  public Member.Type type() {
    return type;
  }

  @Override
  public Member.State state() {
    return state;
  }

  AbstractMemberCoordinator state(Member.State state) {
    this.state = state;
    return this;
  }

  @Override
  public CompletableFuture<Void> open() {
    open = true;
    return CompletableFuture.completedFuture(null);
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
