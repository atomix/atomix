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
package io.atomix.protocols.phi;

import io.atomix.event.ListenerService;
import io.atomix.utils.Identifier;

/**
 * Failure detection service.
 */
public interface FailureDetectionService<T extends Identifier>
    extends ListenerService<FailureDetectionEvent<T>, FailureDetectionEventListener<T>> {

  /**
   * Closes the service.
   */
  void close();

  /**
   * Failure detections service builder.
   *
   * @param <T> the node identifier type
   */
  interface Builder<T extends Identifier> extends io.atomix.utils.Builder<FailureDetectionService<T>> {
  }
}
