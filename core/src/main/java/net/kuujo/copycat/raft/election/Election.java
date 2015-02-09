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
package net.kuujo.copycat.raft.election;

import net.kuujo.copycat.EventListener;

/**
 * Copycat leader election.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Election {

  /**
   * Election status.
   */
  public static enum Status {

    /**
     * Indicates that the election is in progress.
     */
    IN_PROGRESS,

    /**
     * Indicates that the election is complete.
     */
    COMPLETE

  }

  /**
   * Returns the current election status.
   *
   * @return The current election status.
   */
  Status status();

  /**
   * Returns the current election term.
   *
   * @return The current election term.
   */
  long term();

  /**
   * Returns the current election result, if any.
   *
   * @return The current election result or {@code null} if the election is not complete.
   */
  ElectionResult result();

  /**
   * Registers an election completion listener.
   *
   * @param listener The election listener to run once the election is complete.
   * @return The election instance.
   */
  Election addListener(EventListener<ElectionEvent> listener);

  /**
   * Unregisters an election completion listener.
   *
   * @param listener The election listener to unregister.
   * @return The election instance.
   */
  Election removeListener(EventListener<ElectionEvent> listener);

}
