// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.serializer.serializers;

import io.atomix.utils.serializer.Serializer;

/**
 * Default serializers.
 */
public class DefaultSerializers {

  /**
   * Basic serializer.
   */
  public static final Serializer BASIC = Serializer.builder().build();

  private DefaultSerializers() {
  }
}
