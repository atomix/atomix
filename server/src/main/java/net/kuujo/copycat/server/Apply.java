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
package net.kuujo.copycat.server;

import net.kuujo.copycat.Operation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for applying operations to the state machine.
 * <p>
 * This annotation should be used to identify {@link StateMachine} methods for handling
 * {@link Commit commits} submitted to the Raft cluster.
 * <p>
 * When an operation is submitted to the Raft cluster and persisted and replicated, the resulting {@link Commit}
 * will be applied to the state machine on each {@link RaftServer} and the result from the
 * leader will be returned to the client that submitted the operation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Apply {

  /**
   * The operations to apply.
   */
  Class<? extends Operation>[] value();

  /**
   * Indicates that all commands should be applied.
   */
  static class All implements Operation {
  }

}
