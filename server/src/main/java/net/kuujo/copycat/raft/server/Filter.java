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
package net.kuujo.copycat.raft.server;

import net.kuujo.copycat.raft.log.Compaction;
import net.kuujo.copycat.raft.Command;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for filtering committed operations out of the Raft log.
 * <p>
 * This annotation should be used to identify {@link StateMachine} methods for filtering
 * {@link net.kuujo.copycat.raft.log.Entry entries} out of the Raft {@link net.kuujo.copycat.raft.log.Log}. Once an entry has been
 * committed to the log, the entry becomes available for log compaction. The Raft log will periodically compact itself
 * by consulting the state machine implementation on which entries can be removed from the log. Methods annotated with
 * this annotation should return a boolean value indicating whether the provided {@link Commit}
 * can be safely removed from the log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Filter {

  /**
   * The command types to filter.
   */
  Class<? extends Command>[] value() default {};

  /**
   * The filter compaction type.
   */
  Compaction.Type compaction() default Compaction.Type.MAJOR;

  /**
   * Indicates that all commands should be applied to the filter.
   */
  static class All implements Command {
  }

}
