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
package net.kuujo.copycat.impl;

import net.kuujo.copycat.*;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.internal.AbstractResource;

import java.util.concurrent.CompletableFuture;

/**
 * State log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultStateLog<T> extends AbstractResource implements StateLog<T> {
  private EventHandler<T, ?> consumer;

  public DefaultStateLog(String name, Coordinator coordinator, Cluster cluster, CopycatContext context) {
    super(name, coordinator, cluster, context);
  }

  @SuppressWarnings("unchecked")
  private Object handle(Object entry) {
    if (consumer != null) {
      return consumer.handle((T) entry);
    }
    return null;
  }

  @Override
  public StateLog<T> consumer(EventHandler<T, ?> consumer) {
    this.consumer = consumer;
    return this;
  }

  @Override
  public <U> CompletableFuture<U> submit(T command) {
    return context.submit(command, new SubmitOptions().withConsistent(true).withPersistent(true));
  }

}
