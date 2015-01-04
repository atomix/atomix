/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.coordinator.MemberCoordinator;
import net.kuujo.copycat.cluster.manager.MemberManager;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Internal cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedMember implements MemberManager {
  protected static final String EXECUTE_TOPIC = "execute";
  protected static final int USER_ID = 0;
  protected static final int EXECUTE_ID = -1;
  protected final int id;
  private final MemberInfo info;
  private final MemberCoordinator coordinator;
  protected final Serializer serializer;
  protected final Executor executor;

  public CoordinatedMember(int id, MemberInfo info, MemberCoordinator coordinator, Serializer serializer, Executor executor) {
    this.id = id;
    this.info = info;
    this.coordinator = coordinator;
    this.serializer = serializer;
    this.executor = executor;
  }

  /**
   * Returns the member info.
   */
  MemberInfo info() {
    return info;
  }

  /**
   * Returns the member coordinator.
   */
  public MemberCoordinator coordinator() {
    return coordinator;
  }

  @Override
  public String uri() {
    return info.uri();
  }

  @Override
  public Type type() {
    return info.type();
  }

  @Override
  public State state() {
    return info.state();
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    return send(topic, USER_ID, message, serializer);
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, int id, T message, Serializer serializer) {
    return coordinator.send(topic, this.id, id, serializer.writeObject(message)).thenApplyAsync(serializer::readObject, executor);
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return send(EXECUTE_TOPIC, EXECUTE_ID, task, serializer);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    return send(EXECUTE_TOPIC, EXECUTE_ID, task, serializer);
  }

  @Override
  public String toString() {
    return String.format("%s[uri=%s]", getClass().getCanonicalName(), coordinator.uri());
  }

}
