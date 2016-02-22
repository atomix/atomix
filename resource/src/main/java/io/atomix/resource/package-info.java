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
 * limitations under the License
 */

/**
 * Base classes and interfaces for creating and operating on replicated state machines in Atomix.
 * <p>
 * This package provides base classes for Atomix distributed resources. At their core, distributed resources are standalone
 * Copycat {@link io.atomix.copycat.server.StateMachine} implementations which can be accessed by a high-level proxy API.
 * All resources can be run as state machines in a Copycat cluster or multiple state machines in a single cluster can be
 * managed by the Atomix resource manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
package io.atomix.resource;
