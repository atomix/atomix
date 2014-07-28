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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a stateful field.<p>
 *
 * This annotation should be used in conjunction with the
 * {@link AnnotatedStateMachine} to annotate a field within the
 * state machine as being persisted for snapshots. The state machine
 * will automatically store the current state of the annotated field
 * when a snapshot of the state machine is take. When a snapshot is
 * installed, the field will be populated with the snapshot state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Stateful {
}
