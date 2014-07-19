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
package net.kuujo.copycat;

/**
 * Four states in which a CopyCat replica can be at any given time.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public enum CopyCatState {

  /**
   * State used when the replica is performing startup operations.
   */
  START,

  /**
   * State in which the replica receives replicated data from a leader.
   */
  FOLLOWER,

  /**
   * State in which the replica is a candidate in an election.
   */
  CANDIDATE,

  /**
   * State in which the replica is the leader of the cluster and replicates
   * data to followers throughout the cluster.
   */
  LEADER;

}
