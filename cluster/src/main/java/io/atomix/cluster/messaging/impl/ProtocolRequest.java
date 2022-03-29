// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.cluster.messaging.impl;

import com.google.common.base.MoreObjects;
import io.atomix.utils.misc.ArraySizeHashPrinter;
import io.atomix.utils.net.Address;

/**
 * Internal request message.
 */
public final class ProtocolRequest extends ProtocolMessage {
  private final Address sender;
  private final String subject;

  public ProtocolRequest(long id, Address sender, String subject, byte[] payload) {
    super(id, payload);
    this.sender = sender;
    this.subject = subject;
  }

  @Override
  public Type type() {
    return Type.REQUEST;
  }

  public String subject() {
    return subject;
  }

  public Address sender() {
    return sender;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", id())
        .add("subject", subject)
        .add("sender", sender)
        .add("payload", ArraySizeHashPrinter.of(payload()))
        .toString();
  }
}
