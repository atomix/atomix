/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.group.internal;

import io.atomix.copycat.Command;

import java.util.concurrent.CompletableFuture;

/**
 * Group submitter.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@FunctionalInterface
public interface GroupSubmitter {

  /**
   * Submit a command to the cluster.
   *
   * @param command The command to submit.
   * @param <T> The command type.
   * @param <U> The result type.
   * @return A completable future to be completed with the command result.
   */
  <T extends Command<U>, U> CompletableFuture<U> submit(T command);

}
