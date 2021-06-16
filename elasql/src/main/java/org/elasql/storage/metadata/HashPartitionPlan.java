package org.elasql.storage.metadata;

import java.util.HashSet;

import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;

public class HashPartitionPlan extends PartitionPlan {
	
	private int numOfParts;
	private HashSet<PrimaryKey> fullyReplicatedKeys;
	
	public HashPartitionPlan() {
		numOfParts = PartitionMetaMgr.NUM_PARTITIONS;
		fullyReplicatedKeys = new HashSet<PrimaryKey>();
	}
	
	public HashPartitionPlan(int numberOfPartitions) {
		numOfParts = numberOfPartitions;
	}

	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		return fullyReplicatedKeys.contains(key);
	}
	
	@Override
	public void setFullyReplicatedKey(PrimaryKey key) {
		fullyReplicatedKeys.add(key);
	}

	@Override
	public int getPartition(PrimaryKey key) {
		return key.hashCode() % numOfParts;
	}
	
	@Override
	public int numberOfPartitions() {
		return numOfParts;
	}

	@Override
	public PartitionPlan getBasePlan() {
		return this;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		new UnsupportedOperationException();
	}
	
	@Override
	public PartitioningKey getPartitioningKey(PrimaryKey key) {
		return PartitioningKey.fromPrimaryKey(key);
	}
}
