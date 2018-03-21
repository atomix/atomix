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
package io.atomix.protocols.raft.impl;

import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.impl.DefaultServiceContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Raft service registry.
 */
public class RaftServiceRegistry implements Iterable<DefaultServiceContext> {
  private final Map<String, SortedMap<Integer, DefaultServiceContext>> services = new ConcurrentHashMap<>();

  /**
   * Registers a new service.
   *
   * @param service the service to register
   */
  public void registerService(DefaultServiceContext service) {
    services.computeIfAbsent(service.serviceName(), name -> new ConcurrentSkipListMap<>())
        .put(service.revision().revision(), service);
  }

  /**
   * Gets a registered service by revision.
   *
   * @param name the service name
   * @param revision the service revision
   * @return the registered service
   */
  public DefaultServiceContext getRevision(String name, ServiceRevision revision) {
    Map<Integer, DefaultServiceContext> revisions = services.get(name);
    return revisions != null ? revisions.get(revision.revision()) : null;
  }

  /**
   * Gets the current revision for a registered service.
   *
   * @param name the service name
   * @return the current revision for the given service
   */
  public DefaultServiceContext getCurrentRevision(String name) {
    SortedMap<Integer, DefaultServiceContext> revisions = services.get(name);
    if (revisions == null) {
      return null;
    }

    Integer lastKey = revisions.lastKey();
    if (lastKey == null) {
      return null;
    }
    return revisions.get(lastKey);
  }

  /**
   * Returns all revisions for the given service.
   *
   * @param name the service name
   * @return a collection of revisions for the given service
   */
  public Collection<DefaultServiceContext> getRevisions(String name) {
    SortedMap<Integer, DefaultServiceContext> revisions = services.get(name);
    return revisions != null ? revisions.values() : Collections.emptyList();
  }

  @Override
  public Iterator<DefaultServiceContext> iterator() {
    return services.values().stream()
        .flatMap(s -> s.values().stream())
        .iterator();
  }
}
