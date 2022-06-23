package com.orangerhymelabs.helenus.cassandra.meta;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenus.cassandra.migration.MigrationConfiguration;
import com.orangerhymelabs.helenus.cassandra.migration.MigrationException;

public class CassandraMetadataStrategy
implements MetadataStrategy
{
	private static final String MIGRATIONS_KEY = "migrations";

	private MigrationConfiguration config;

	public CassandraMetadataStrategy(MigrationConfiguration configuration)
	{
		super();
		this.config = configuration;
	}

	public boolean exists(Session session)
	{
		ResultSet rs = session.execute(String.format("select count(*) from system_schema.tables where keyspace_name='%s' and table_name='%s'", session.getLoggedKeyspace(), config.getMetadataTable()));
		return (rs.one().getLong(0) > 0);
	}

	public int getCurrentVersion(Session session)
	{
		ResultSet rs = session.execute(String.format("select version from %s.%s where name = %s limit 1", session.getLoggedKeyspace(), config.getMetadataTable(), MIGRATIONS_KEY));
		return rs.one().getInt(0);
	}

	public void initialize(Session session)
	{
		ResultSet rs = session.execute(String.format("create table if not exists %s.%s (" +
			"name text," +
			"version int," +
			"description text," +
			"script text," +
			"hash text," +
			"installed_on timestamp," +
			"exectime_ms bigint," +
			"was_successful boolean," +
			"primary key ((name), installed_on, version)" +
		") with clustering order by (installed_on DESC, version DESC);",
		config.getKeyspace(), config.getMetadataTable()));

		if (!rs.wasApplied())
		{
			throw new MigrationException("Migration metadata intialization failed");
		}
	}

	public void update(Session session, AppliedMigration metadata)
	{
		PreparedStatement ps = session.prepare(String.format("insert into %s.%s (name, version, description, script, hash, installed_on, exectime_ms, was_successful) values (?, ?, ?, ?, ?, ?, ?, ?)",
			config.getKeyspace(), config.getMetadataTable()));
		ResultSet rs = session.execute(ps.bind(MIGRATIONS_KEY, metadata.getVersion(), metadata.getDescription(), metadata.getScript(), metadata.getHash(), metadata.getAppliedAt(), metadata.getExecutionDurationMillis(), metadata.wasSuccessful()));

		if (!rs.wasApplied())
		{
			throw new MigrationException("Failed to update migration metadata.");
		}
	}
}
