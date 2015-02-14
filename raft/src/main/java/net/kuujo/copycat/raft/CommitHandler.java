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
package net.kuujo.copycat.raft;

import java.nio.ByteBuffer;

/**
 * Raft commit handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@FunctionalInterface
public interface CommitHandler {

  /**
   * Commits an entry.
   *
   * @param term The entry term.
   * @param index The entry index, or {@code null} if the entry is not a persistent entry.
   * @param entry The entry.
   * @return The commit output.
   */
  ByteBuffer commit(long term, Long index, ByteBuffer entry);

}
