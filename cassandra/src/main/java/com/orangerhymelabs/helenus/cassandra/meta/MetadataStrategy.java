package com.orangerhymelabs.helenus.cassandra.meta;

import com.datastax.driver.core.Session;

public interface MetadataStrategy
{
	public boolean exists(Session session);
	public int getCurrentVersion(Session session);
	public void initialize(Session session);
	public void update(Session session, AppliedMigration metadata);
}
