/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.messaging;

import java.util.concurrent.CompletableFuture;

/**
 * Message subscription.
 */
public interface Subscription {

  /**
   * Returns the subscription topic.
   *
   * @return the subscription topic
   */
  String topic();

  /**
   * Closes the subscription, causing it to be unregistered.
   *
   * @return a future to be completed once the subscription has been closed
   */
  CompletableFuture<Void> close();

}
