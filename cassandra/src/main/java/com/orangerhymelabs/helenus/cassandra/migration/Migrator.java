package com.orangerhymelabs.helenus.cassandra.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenus.cassandra.meta.CassandraMetadataStrategy;
import com.orangerhymelabs.helenus.cassandra.meta.AppliedMigration;
import com.orangerhymelabs.helenus.cassandra.meta.MetadataStrategy;

/**
 * @author tfredrich
 *
 */
public class Migrator
{
	private static final Logger LOG = LoggerFactory.getLogger(Migrator.class);

	private static final int UNINITIALIZED = -1;

	private MigrationConfiguration configuration;
	private Set<Migration> migrations = new HashSet<Migration>();
	private MetadataStrategy metadata;

	public Migrator()
	{
		super();
		setConfiguration(new MigrationConfiguration());
	}
	
	public void setConfiguration(MigrationConfiguration configuration)
	{
		this.configuration = configuration;
		this.metadata = new CassandraMetadataStrategy(configuration);
	}

	public MigrationConfiguration getConfiguration()
	{
		return configuration;
	}

	public Migrator register(Migration migration)
	{
		migrations.add(migration);
		return this;
	}

	public Migrator registerAll(Collection<Migration> migrations)
	{
		this.migrations.addAll(migrations);
		return this;
	}

	public void migrate(final Session session)
	throws IOException
	{
		int currentVersion = getCurrentVersion(session);

		if (currentVersion < 0)
		{
			currentVersion = initializeMetadata(session);
		}

		Collection<Migration> scriptMigrations = discoverMigrationScripts();
		List<Migration> allMigrations = new ArrayList<Migration>(migrations.size() + scriptMigrations.size());
		allMigrations.addAll(migrations);
		allMigrations.addAll(scriptMigrations);
		Collections.sort(allMigrations, Collections.reverseOrder());
		int latestVersion = allMigrations.get(allMigrations.size() -1).getVersion();

		if (currentVersion < latestVersion)
		{
			LOG.info(String.format("Database needs migration from current version %d to version %d", currentVersion, latestVersion));
			boolean wasSuccessful = process(session, allMigrations, currentVersion, latestVersion);

			if (!wasSuccessful)
			{
				throw new MigrationException("Migration aborted");
			}

			LOG.info("Database migration completed successfuly.");
		}
	}

	private boolean process(final Session session, final List<Migration> allMigrations, final int from, final int to)
	{
		for (Migration migration : allMigrations)
		{
			if (migration.isApplicable(from, to))
			{
				LOG.info("Migrating database to version " + migration.getVersion());
				long startTimeMillis = System.currentTimeMillis();
				boolean wasSuccessful = migration.migrate(session);
				long executionTime = System.currentTimeMillis() - startTimeMillis;
				metadata.update(session, new AppliedMigration(migration, executionTime, wasSuccessful));

				if (!wasSuccessful)
				{
					return false;
				}
			}
		}

		return true;
	}

	private Collection<Migration> discoverMigrationScripts()
	throws IOException
	{
		return new ClasspathMigrationLoader().load(configuration);
	}

	private int getCurrentVersion(Session session)
	{
		if (!metadata.exists(session))
		{
			return UNINITIALIZED;
		}

		return metadata.getCurrentVersion(session);
	}

	private int initializeMetadata(Session session)
	{
		metadata.initialize(session);
		return 0;
	}
}
