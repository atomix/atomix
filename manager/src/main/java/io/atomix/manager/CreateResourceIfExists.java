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
package io.atomix.manager;

import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.resource.ResourceStateMachine;

/**
 * Create resource if exists command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=39)
public class CreateResourceIfExists extends CreateResource {

  public CreateResourceIfExists() {
  }

  /**
   * @throws NullPointerException if {@code path} or {@code type} are null
   */
  public CreateResourceIfExists(String key, Class<? extends ResourceStateMachine> type) {
    super(key, type);
  }

}
