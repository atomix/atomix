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
package io.atomix.core.iterator.impl;

/**
 * Iterator close function.
 */
@FunctionalInterface
public interface CloseFunction<S> {

  /**
   * Closes the iterator using the given service proxy.
   *
   * @param service the service proxy
   * @param iteratorId the iterator ID
   */
  void close(S service, long iteratorId);
}
