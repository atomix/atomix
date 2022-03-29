// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.event;

/**
 * Basis for components which need to export listener mechanism.
 */
public abstract class AbstractListenerManager<E extends Event, L extends EventListener<E>> implements ListenerService<E, L> {

  protected final ListenerRegistry<E, L> listenerRegistry = new ListenerRegistry<>();

  @Override
  public void addListener(L listener) {
    listenerRegistry.addListener(listener);
  }

  @Override
  public void removeListener(L listener) {
    listenerRegistry.removeListener(listener);
  }

  /**
   * Posts the specified event to the local event dispatcher.
   *
   * @param event event to be posted; may be null
   */
  protected void post(E event) {
    listenerRegistry.process(event);
  }

}
