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
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Internal cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatedMember implements Member {
  private final Member parent;
  protected final ExecutionContext context;

  protected CoordinatedMember(Member parent, ExecutionContext context) {
    this.parent = parent;
    this.context = context;
  }

  @Override
  public String uri() {
    return parent.uri();
  }

  @Override
  public State state() {
    return null;
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    return parent.send(topic, message);
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    return parent.execute(task);
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    return parent.submit(task);
  }

}
