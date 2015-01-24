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
package net.kuujo.copycat.cluster.internal.manager;

import net.kuujo.copycat.util.Managed;
import net.kuujo.copycat.cluster.Cluster;

/**
 * Resource cluster.<p>
 *
 * The cluster contains state information defining the current cluster configuration. Note that cluster configuration
 * state is potential stale at any given point in time.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterManager extends Cluster, Managed<ClusterManager> {

  @Override
  MemberManager leader();

  @Override
  LocalMemberManager member();

  @Override
  MemberManager member(String uri);

}
