// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.event.impl;

import io.atomix.primitive.event.EventType;
import io.atomix.utils.AbstractIdentifier;

/**
 * Default Raft event identifier.
 */
public class DefaultEventType extends AbstractIdentifier<String> implements EventType {
  private DefaultEventType() {
  }

  public DefaultEventType(String value) {
    super(value);
  }
}
