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
package net.kuujo.copycat.raft.protocol;

import net.kuujo.copycat.util.BuilderPool;

/**
 * Raft state queries read system state.
 * <p>
 * Queries are submitted by clients to a Raft server to read Raft cluster-wide state. In contrast to
 * {@link Command commands}, queries allow for more flexible
 * {@link ConsistencyLevel consistency levels} that trade consistency for performance.
 * <p>
 * All queries must specify a {@link #consistency()} with which to execute the query. The provided consistency level
 * dictates how queries are submitted to the Raft cluster. Higher consistency levels like
 * {@link ConsistencyLevel#LINEARIZABLE} and {@link ConsistencyLevel#LINEARIZABLE_LEASE}
 * are forwarded to the cluster leader, while lower levels are allowed to read from followers for higher throughput.
 * <p>
 * By default, all queries should use the strongest consistency level, {@link ConsistencyLevel#LINEARIZABLE}.
 * It is essential that users understand the trade-offs in the various consistency levels before using them.
 *
 * @see ConsistencyLevel
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Query<T> extends Operation<T> {

  /**
   * Returns the query consistency level.
   * <p>
   * The consistency will dictate how the query is executed on the server state. Stronger consistency levels can guarantee
   * linearizability in all or most cases, while weaker consistency levels trade linearizability for more performant
   * reads from followers. Consult the {@link ConsistencyLevel} documentation for more information
   * on the different consistency levels.
   * <p>
   * By default, this method enforces strong consistency with the {@link ConsistencyLevel#LINEARIZABLE} consistency level.
   *
   * @return The query consistency level.
   */
  default ConsistencyLevel consistency() {
    return ConsistencyLevel.LINEARIZABLE;
  }

  /**
   * Base builder for queries.
   */
  static abstract class Builder<T extends Builder<T, U, V>, U extends Query<V>, V> extends Operation.Builder<T, U, V> {
    protected U query;

    protected Builder(BuilderPool<T, U> pool) {
      super(pool);
    }

    @Override
    protected void reset(U query) {
      super.reset(query);
      this.query = query;
    }

    @Override
    public U build() {
      close();
      return query;
    }
  }

}
