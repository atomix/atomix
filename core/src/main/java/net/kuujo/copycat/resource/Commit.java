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
package net.kuujo.copycat.resource;

/**
 * Protocol commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Commit<T extends Command> {
  private long index;
  private long timestamp;
  private T command;

  /**
   * Sets the index.
   */
  Commit withIndex(long index) {
    this.index = index;
    return this;
  }

  /**
   * Returns the commit index.
   *
   * @return The commit index.
   */
  public long index() {
    return index;
  }

  /**
   * Sets the timestamp.
   */
  Commit withTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  /**
   * Returns the commit timestamp.
   *
   * @return the commit timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Sets the command.
   */
  Commit withCommand(T command) {
    this.command = command;
    return this;
  }

  /**
   * Returns the commit type.
   *
   * @return The commit type.
   */
  @SuppressWarnings("unchecked")
  public Class<T> type() {
    return (Class<T>) command.getClass();
  }

  /**
   * Returns the command.
   *
   * @return The command.
   */
  public T command() {
    return command;
  }

}
