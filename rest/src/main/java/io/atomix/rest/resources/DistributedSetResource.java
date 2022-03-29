// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.resources;

import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSetConfig;
import io.atomix.core.set.DistributedSetType;
import io.atomix.rest.AtomixResource;

import javax.ws.rs.Path;

/**
 * Distributed set resource.
 */
@AtomixResource
@Path("/set")
public class DistributedSetResource extends DistributedCollectionResource<AsyncDistributedSet<String>, DistributedSetConfig> {
  public DistributedSetResource() {
    super(DistributedSetType.instance());
  }
}
