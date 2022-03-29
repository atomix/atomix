// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.impl;

import io.atomix.protocols.raft.service.RaftServiceContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Raft service registry.
 */
public class RaftServiceRegistry implements Iterable<RaftServiceContext> {
  private final Map<String, RaftServiceContext> services = new ConcurrentHashMap<>();

  /**
   * Registers a new service.
   *
   * @param service the service to register
   */
  public void registerService(RaftServiceContext service) {
    services.put(service.serviceName(), service);
  }

  /**
   * Unregisters the given service.
   *
   * @param service the service to unregister
   */
  public void unregisterService(RaftServiceContext service) {
    services.remove(service.serviceName());
  }

  /**
   * Gets a registered service by name.
   *
   * @param name the service name
   * @return the registered service
   */
  public RaftServiceContext getService(String name) {
    return services.get(name);
  }

  @Override
  public Iterator<RaftServiceContext> iterator() {
    return services.values().iterator();
  }

  /**
   * Returns a copy of the services registered in the registry.
   *
   * @return a copy of the registered services
   */
  public Collection<RaftServiceContext> copyValues() {
    return new ArrayList<>(services.values());
  }
}
