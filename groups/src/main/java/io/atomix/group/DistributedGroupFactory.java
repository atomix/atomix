/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.group;

import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.group.internal.MembershipGroup;
import io.atomix.group.internal.GroupCommands;
import io.atomix.group.internal.GroupState;
import io.atomix.resource.ResourceFactory;
import io.atomix.resource.ResourceStateMachine;

import java.util.Properties;

/**
 * Distributed group factory.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DistributedGroupFactory implements ResourceFactory<DistributedGroup> {

  @Override
  public SerializableTypeResolver createSerializableTypeResolver() {
    return new GroupCommands.TypeResolver();
  }

  @Override
  public ResourceStateMachine createStateMachine(Properties config) {
    return new GroupState(config);
  }

  @Override
  public DistributedGroup createInstance(CopycatClient client, Properties options) {
    return new MembershipGroup(client, options);
  }

}
