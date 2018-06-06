/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.profile;

import io.atomix.core.AtomixConfig;
import io.atomix.utils.NamedType;

/**
 * Atomix profile.
 */
public interface Profile extends NamedType {

  /**
   * The consensus profile configures an Atomix instance with a Raft system partition and a multi-Raft data partition group.
   */
  Profile CONSENSUS = new ConsensusProfile();

  /**
   * The data grid profile configures an Atomix instance with a primary-backup system partition if no system partition
   * is configured, and a primary-backup data partition group.
   */
  Profile DATA_GRID = new DataGridProfile();

  /**
   * The client profile does not change the configuration of a node. It is intended only for code clarity.
   */
  Profile CLIENT = new ClientProfile();

  /**
   * Configures the Atomix instance.
   *
   * @param config the Atomix configuration
   */
  void configure(AtomixConfig config);

}
