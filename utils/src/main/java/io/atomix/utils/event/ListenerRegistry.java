// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base implementation of an event sink and a registry capable of tracking
 * listeners and dispatching events to them as part of event sink processing.
 */
public class ListenerRegistry<E extends Event, L extends EventListener<E>>
    implements ListenerService<E, L>, EventSink<E> {

  private static final long LIMIT = 1_800; // ms

  private final Logger log = LoggerFactory.getLogger(getClass());

  private long lastStart;
  private L lastListener;

  /**
   * Set of listeners that have registered.
   */
  protected final Set<L> listeners = new CopyOnWriteArraySet<>();

  @Override
  public void addListener(L listener) {
    checkNotNull(listener, "Listener cannot be null");
    listeners.add(listener);
  }

  @Override
  public void removeListener(L listener) {
    checkNotNull(listener, "Listener cannot be null");
    if (!listeners.remove(listener)) {
      log.warn("Listener {} not registered", listener);
    }
  }

  @Override
  public void process(E event) {
    for (L listener : listeners) {
      try {
        lastListener = listener;
        lastStart = System.currentTimeMillis();
        if (listener.isRelevant(event)) {
          listener.event(event);
        }
        lastStart = 0;
      } catch (Exception error) {
        reportProblem(event, error);
      }
    }
  }

  @Override
  public void onProcessLimit() {
    if (lastStart > 0) {
      long duration = System.currentTimeMillis() - lastStart;
      if (duration > LIMIT) {
        log.error("Listener {} exceeded execution time limit: {} ms; ejected",
            lastListener.getClass().getName(),
            duration);
        removeListener(lastListener);
      }
      lastStart = 0;
    }
  }

  /**
   * Reports a problem encountered while processing an event.
   *
   * @param event event being processed
   * @param error error encountered while processing
   */
  protected void reportProblem(E event, Throwable error) {
    log.warn("Exception encountered while processing event " + event, error);
  }

}
