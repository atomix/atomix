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
package net.kuujo.copycat.log;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.ManagedCluster;
import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.protocol.Persistence;
import net.kuujo.copycat.protocol.Protocol;
import net.kuujo.copycat.protocol.ProtocolFilter;
import net.kuujo.copycat.util.ExecutionContext;
import net.kuujo.copycat.util.Managed;

import java.util.concurrent.CompletableFuture;

/**
 * Distributed commit log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class CommitLog<KEY, VALUE> implements Managed<CommitLog<KEY, VALUE>> {

  /**
   * Returns a new commit log builder.
   *
   * @param <KEY> The commit log key type.
   * @param <VALUE> The commit log entry type.
   * @return The commit log builder.
   */
  public static <KEY, VALUE> Builder<KEY, VALUE> builder() {
    return new Builder<>();
  }

  /**
   * Returns the commit log name.
   *
   * @return The commit log name.
   */
  public abstract String name();

  /**
   * Returns the commit log cluster.
   *
   * @return The commit log cluster.
   */
  public abstract Cluster cluster();

  /**
   * Commits an entry to the log.
   *
   * @param key The entry key.
   * @param value The entry value.
   * @param persistence The entry persistence level.
   * @param consistency The commit consistency level.
   * @param <RESULT> The commit result type.
   * @return A completable future to be completed with the commit result.
   */
  public abstract <RESULT> CompletableFuture<RESULT> commit(KEY key, VALUE value, Persistence persistence, Consistency consistency);

  /**
   * Commits an entry to the log.
   *
   * @param key The entry key.
   * @param value The entry value.
   * @param persistence The entry persistence level.
   * @param consistency The commit consistency level.
   * @return A completable future to be completed with the commit result.
   */
  protected abstract CompletableFuture<Buffer> commit(Buffer key, Buffer value, Persistence persistence, Consistency consistency);

  /**
   * Registers a commit handler.
   *
   * @param handler The commit handler.
   * @param <RESULT> The commit result type.
   * @return The commit log.
   */
  public abstract <RESULT> CommitLog<KEY, VALUE> handler(CommitHandler<KEY, VALUE, RESULT> handler);

  /**
   * Registers a raw commit handler.
   *
   * @param handler The raw commit handler.
   * @return The commit log.
   */
  protected abstract CommitLog<KEY, VALUE> handler(RawCommitHandler handler);

  /**
   * Sets a filter on the commit log.
   *
   * @param filter The commit log filter.
   * @return The commit log.
   */
  protected abstract CommitLog<KEY, VALUE> filter(ProtocolFilter filter);

  /**
   * Commit log builder.
   *
   * @param <KEY> The log key.
   * @param <VALUE> The log value.
   */
  public static class Builder<KEY, VALUE> implements net.kuujo.copycat.Builder<CommitLog<KEY, VALUE>> {
    protected final CommitLogConfig config = new CommitLogConfig();

    /**
     * Sets the commit log name.
     *
     * @param name The commit log name.
     * @return The commit log builder.
     */
    public Builder<KEY, VALUE> withName(String name) {
      config.setName(name);
      return this;
    }

    /**
     * Sets the commit log cluster.
     *
     * @param cluster The commit log cluster.
     * @return The commit log builder.
     */
    public Builder<KEY, VALUE> withCluster(ManagedCluster cluster) {
      config.setCluster(cluster);
      return this;
    }

    /**
     * Sets the commit log protocol.
     *
     * @param protocol The commit log protocol.
     * @return The commit log builder.
     */
    public Builder<KEY, VALUE> withProtocol(Protocol protocol) {
      config.setProtocol(protocol);
      return this;
    }

    @Override
    public CommitLog<KEY, VALUE> build() {
      config.resolve();
      return new CommitLogPartition<>(config.getName(), config.getCluster(), config.getProtocol(), new ExecutionContext(config.getName()));
    }
  }

}
