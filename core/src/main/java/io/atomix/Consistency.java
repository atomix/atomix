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
package io.atomix;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;

/**
 * Resource consistency constraints.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum Consistency {

  /**
   * None consistency level.
   */
  NONE {
    @Override
    public Command.ConsistencyLevel writeConsistency() {
      return Command.ConsistencyLevel.NONE;
    }

    @Override
    public Query.ConsistencyLevel readConsistency() {
      return Query.ConsistencyLevel.CAUSAL;
    }
  },

  /**
   * Client consistency level.
   */
  CLIENT {
    @Override
    public Command.ConsistencyLevel writeConsistency() {
      return Command.ConsistencyLevel.SEQUENTIAL;
    }

    @Override
    public Query.ConsistencyLevel readConsistency() {
      return Query.ConsistencyLevel.CAUSAL;
    }
  },

  /**
   * Ordered consistency level.
   */
  ORDERED {
    @Override
    public Command.ConsistencyLevel writeConsistency() {
      return Command.ConsistencyLevel.SEQUENTIAL;
    }

    @Override
    public Query.ConsistencyLevel readConsistency() {
      return Query.ConsistencyLevel.SEQUENTIAL;
    }
  },

  /**
   * Strong consistency level.
   */
  STRONG {
    @Override
    public Command.ConsistencyLevel writeConsistency() {
      return Command.ConsistencyLevel.LINEARIZABLE;
    }

    @Override
    public Query.ConsistencyLevel readConsistency() {
      return Query.ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }
  };

  /**
   * Returns the write consistency level.
   *
   * @return The write consistency level.
   */
  public abstract Command.ConsistencyLevel writeConsistency();

  /**
   * Returns the read consistency level.
   *
   * @return The read consistency level.
   */
  public abstract Query.ConsistencyLevel readConsistency();

}
