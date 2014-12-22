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
package net.kuujo.copycat.internal.cluster;

import net.kuujo.copycat.CopycatContext;
import net.kuujo.copycat.Task;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.concurrent.CompletableFuture;

/**
 * Resource member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceMember implements Member {
  private final Member member;
  private final CopycatContext context;
  private final ExecutionContext executor;

  ResourceMember(Member member, CopycatContext context, ExecutionContext executor) {
    this.member = member;
    this.context = context;
    this.executor = executor;
  }

  @Override
  public String uri() {
    return member.uri();
  }

  @Override
  public Type type() {
    return member.type();
  }

  @Override
  public State state() {
    return member.state();
  }

  @Override
  public <T, U> CompletableFuture<U> send(String topic, T message) {
    CompletableFuture<U> future = new CompletableFuture<>();
    context.execute(() -> {
      member.<T, U>send(topic, message).whenComplete((result, error) -> {
        if (error == null) {
          executor.execute(() -> future.complete(result));
        } else {
          executor.execute(() -> future.completeExceptionally(error));
        }
      });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> execute(Task<Void> task) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.execute(() -> {
      member.execute(task).whenComplete((result, error) -> {
        if (error == null) {
          executor.execute(() -> future.complete(result));
        } else {
          executor.execute(() -> future.completeExceptionally(error));
        }
      });
    });
    return future;
  }

  @Override
  public <T> CompletableFuture<T> submit(Task<T> task) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.execute(() -> {
      member.submit(task).whenComplete((result, error) -> {
        if (error == null) {
          executor.execute(() -> future.complete(result));
        } else {
          executor.execute(() -> future.completeExceptionally(error));
        }
      });
    });
    return future;
  }

  @Override
  public String toString() {
    return String.format("%s[uri=%s]", getClass().getCanonicalName(), uri());
  }

}
