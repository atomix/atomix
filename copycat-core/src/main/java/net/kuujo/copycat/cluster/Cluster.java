package net.kuujo.copycat.cluster;

import java.util.Set;

import net.kuujo.copycat.CopyCatContext;

public interface Cluster {

  CopyCatContext context();

  ClusterConfig config();

  Member localMember();

  Set<Member> members();

  Member member(String address);

}
