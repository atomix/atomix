// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive.protocol.value;

import com.google.common.annotations.Beta;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.protocol.GossipProtocol;
import io.atomix.utils.serializer.Serializer;

/**
 * Value protocol.
 */
@Beta
public interface ValueProtocol extends GossipProtocol {

  /**
   * Returns a new value delegate.
   *
   * @param name the value name
   * @param serializer the value serializer
   * @param managementService the primitive management service
   * @return a new value delegate
   */
  ValueDelegate newValueDelegate(String name, Serializer serializer, PrimitiveManagementService managementService);

}
