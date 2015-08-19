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

import net.kuujo.copycat.io.storage.PersistenceLevel;
import net.kuujo.copycat.util.BuilderPool;

/**
 * Raft state commands modify system state.
 * <p>
 * Commands are submitted by clients to a Raft server and used to modify Raft cluster-wide state. The Raft
 * consensus protocol dictates that commands must be forwarded to the cluster leader and replicated to a majority of
 * followers before being applied to the cluster state. Thus, in contrast to {@link Query queries},
 * commands are not dictated by different consistency levels.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Command<T> extends Operation<T> {

  /**
   * Returns the command storage level.
   * <p>
   * The storage level specifies how the command should be persisted in Raft replicated logs. Copycat's log
   * supports persisting commands to disk or holding them in memory depending on the provided storage level.
   * If the storage level is {@link PersistenceLevel#DISK} then the command will persist across failures, otherwise
   * a failure will result in {@link PersistenceLevel#MEMORY} commands being lost.
   * <p>
   * It's important to note, though, that all commands will be replicated to a majority of the cluster regardless
   * of their storage level.
   *
   * @return The command storage level.
   */
  default PersistenceLevel persistence() {
    return PersistenceLevel.DISK;
  }

  /**
   * Base builder for commands.
   */
  abstract class Builder<T extends Builder<T, U, V>, U extends Command<V>, V> extends Operation.Builder<T, U, V> {
    protected U command;

    protected Builder(BuilderPool<T, U> pool) {
      super(pool);
    }

    @Override
    protected void reset(U command) {
      super.reset(command);
      this.command = command;
    }

    @Override
    public U build() {
      close();
      return command;
    }
  }

}
