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
package net.kuujo.copycat.event;

/**
 * Event constants.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class Events {

  /**
   * Start event constant.
   */
  public static Class<? extends Event> START = StartEvent.class;

  /**
   * Stop event constant.
   */
  public static Class<? extends Event> STOP = StopEvent.class;

  /**
   * Vote cast event constant.
   */
  public static Class<? extends Event> VOTE_CAST = VoteCastEvent.class;

  /**
   * Leader elected event constant.
   */
  public static Class<? extends Event> LEADER_ELECT = LeaderElectEvent.class;

  /**
   * State change event constant.
   */
  public static Class<? extends Event> STATE_CHANGE = StateChangeEvent.class;

  /**
   * Cluster membership change event constant.
   */
  public static Class<? extends Event> MEMBERSHIP_CHANGE = MembershipChangeEvent.class;

}
