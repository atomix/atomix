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

import net.kuujo.copycat.protocol.Commit;

/**
 * Command context.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Context {
  private Commit commit;

  /**
   * Sets the context commit.
   *
   * @param commit The context commit.
   * @return The context.
   */
  Context setCommit(Commit commit) {
    this.commit = commit;
    return this;
  }

  /**
   * Returns the commit index.
   *
   * @return The commit index.
   */
  public long index() {
    return commit.index();
  }

  /**
   * Returns the commit timestamp.
   *
   * @return The commit timestamp.
   */
  public long timestamp() {
    return commit.timestamp();
  }

}
