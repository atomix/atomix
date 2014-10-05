/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol submit handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface SubmitHandler {

  /**
   * Submits a command.
   *
   * @param command The command to submit.
   * @param args A list of command arguments.
   * @param <T> The return type.
   * @return A completable future to be called once the result is received.
   */
  <T> CompletableFuture<T> submit(String command, Object... args);

}
