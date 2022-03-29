// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.backup.serializer.impl;

import io.atomix.utils.serializer.Serializer;

/**
 * Primary-backup serializers.
 */
public final class PrimaryBackupSerializers {
  public static final Serializer PROTOCOL = Serializer.using(PrimaryBackupNamespaces.PROTOCOL);

  private PrimaryBackupSerializers() {
  }
}
