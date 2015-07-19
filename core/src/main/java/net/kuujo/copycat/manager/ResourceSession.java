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
package net.kuujo.copycat.manager;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.ResourceMessage;
import net.kuujo.copycat.raft.Session;

import java.util.concurrent.CompletableFuture;

/**
 * Resource session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResourceSession extends Session {
  private final long id;
  private final Session parent;

  public ResourceSession(long id, Session parent) {
    super(parent.id(), parent.member(), parent.connection());
    this.id = id;
    this.parent = parent;
  }

  @Override
  protected void close() {
    super.close();
  }

  @Override
  protected void expire() {
    super.expire();
  }

  @Override
  public CompletableFuture<Void> publish(Object message) {
    return parent.publish(new ResourceMessage<>(id, message));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListenerContext onReceive(Listener<T> listener) {
    return parent.<ResourceMessage>onReceive(message -> {
      if (message.resource() == id) {
        listener.accept((T) message.message());
      }
    });
  }

}
