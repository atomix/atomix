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
package net.kuujo.copycat.coordination;

import net.kuujo.copycat.Listener;
import net.kuujo.copycat.ListenerContext;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.coordination.state.LeaderElectionCommands;
import net.kuujo.copycat.coordination.state.LeaderElectionState;
import net.kuujo.copycat.resource.ResourceContext;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Asynchronous leader election resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(LeaderElectionState.class)
public class DistributedLeaderElection extends Resource {
  private final Set<Listener<Void>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public DistributedLeaderElection(ResourceContext context) {
    super(context);
    context.session().onReceive(v -> {
      for (Listener<Void> listener : listeners) {
        listener.accept(null);
      }
    });
  }

  /**
   * Registers a listener to be called when this client is elected.
   *
   * @param listener The listener to register.
   * @return A completable future to be completed with the listener context.
   */
  public CompletableFuture<ListenerContext<Void>> onElection(Listener<Void> listener) {
    if (!listeners.isEmpty()) {
      listeners.add(listener);
      return CompletableFuture.completedFuture(new ElectionListenerContext(listener));
    }

    listeners.add(listener);
    return submit(LeaderElectionCommands.Listen.builder().build())
      .thenApply(v -> new ElectionListenerContext(listener));
  }

  /**
   * Change listener context.
   */
  private class ElectionListenerContext implements ListenerContext<Void> {
    private final Listener<Void> listener;

    private ElectionListenerContext(Listener<Void> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(Void event) {
      listener.accept(event);
    }

    @Override
    public void close() {
      synchronized (DistributedLeaderElection.this) {
        listeners.remove(listener);
        if (listeners.isEmpty()) {
          submit(LeaderElectionCommands.Unlisten.builder().build());
        }
      }
    }
  }

}
