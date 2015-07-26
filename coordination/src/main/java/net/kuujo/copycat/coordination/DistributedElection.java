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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.copycat.*;
import net.kuujo.copycat.log.Compaction;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Raft;
import net.kuujo.copycat.raft.Session;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Asynchronous leader election resource.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(DistributedElection.StateMachine.class)
public class DistributedElection extends Resource {
  private final Set<Listener<Void>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public DistributedElection(Raft protocol) {
    super(protocol);
    protocol.session().onReceive(v -> {
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
    return submit(Listen.builder().build())
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
      synchronized (DistributedElection.this) {
        listeners.remove(listener);
        if (listeners.isEmpty()) {
          submit(Unlisten.builder().build());
        }
      }
    }
  }

  /**
   * Abstract election command.
   */
  public static abstract class ElectionCommand<V> implements Command<V>, AlleycatSerializable {
    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
    }

    /**
     * Base reference command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends ElectionCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Listen command.
   */
  @SerializeWith(id=510)
  public static class Listen extends ElectionCommand<Void> {

    /**
     * Returns a new listen command builder.
     *
     * @return A new listen command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Listen command builder.
     */
    public static class Builder extends ElectionCommand.Builder<Builder, Listen, Void> {
      public Builder(BuilderPool<Builder, Listen> pool) {
        super(pool);
      }

      @Override
      protected Listen create() {
        return new Listen();
      }
    }
  }

  /**
   * Unlisten command.
   */
  @SerializeWith(id=510)
  public static class Unlisten extends ElectionCommand<Void> {

    /**
     * Returns a new unlisten command builder.
     *
     * @return A new unlisten command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Unlisten command builder.
     */
    public static class Builder extends ElectionCommand.Builder<Builder, Unlisten, Void> {
      public Builder(BuilderPool<Builder, Unlisten> pool) {
        super(pool);
      }

      @Override
      protected Unlisten create() {
        return new Unlisten();
      }
    }
  }

  /**
   * Async leader election state machine.
   */
  public static class StateMachine extends net.kuujo.copycat.raft.server.StateMachine {
    private long version;
    private Session leader;
    private final List<Commit<Listen>> listeners = new ArrayList<>();

    @Override
    public void close(Session session) {
      if (leader != null && leader.equals(session)) {
        leader = null;
        if (!listeners.isEmpty()) {
          Commit<Listen> leader = listeners.remove(0);
          this.leader = leader.session();
          this.version = leader.index();
          this.leader.publish(true);
        }
      }
    }

    /**
     * Applies listen commits.
     */
    @Apply(Listen.class)
    protected void applyListen(Commit<Listen> commit) {
      if (leader == null) {
        leader = commit.session();
        version = commit.index();
        leader.publish(true);
      } else {
        listeners.add(commit);
      }
    }

    /**
     * Applies listen commits.
     */
    @Apply(Unlisten.class)
    protected void applyUnlisten(Commit<Listen> commit) {
      if (leader != null && leader.equals(commit.session())) {
        leader = null;
        if (!listeners.isEmpty()) {
          Commit<Listen> leader = listeners.remove(0);
          this.leader = leader.session();
          this.version = leader.index();
          this.leader.publish(true);
        }
      }
    }

    /**
     * Filters listen commits.
     */
    @Filter(Listen.class)
    protected boolean filterListen(Commit<Listen> commit, Compaction compaction) {
      return commit.index() >= version;
    }

    /**
     * Filters unlisten commits.
     */
    @Filter(Unlisten.class)
    protected boolean filterUnlisten(Commit<Unlisten> commit, Compaction compaction) {
      return commit.index() >= version;
    }
  }

}
