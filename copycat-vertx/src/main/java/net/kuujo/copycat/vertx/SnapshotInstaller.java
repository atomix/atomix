/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.vertx;

import java.nio.Buffer;

/**
 * Interface for implementing a snapshot installer.<p>
 *
 * This interface should be implemented by state machines in tandem with the
 * {@link SnapshotCreator} interface. State machines which implement these
 * interfaces support log compaction through snapshotting of the machine state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface SnapshotInstaller {

  /**
   * Installs a state machine snapshot.
   *
   * @param snapshot The machine state.
   */
  void installSnapshot(Buffer snapshot);

}
