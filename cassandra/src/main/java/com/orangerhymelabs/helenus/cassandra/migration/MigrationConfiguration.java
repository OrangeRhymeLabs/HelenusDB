package com.orangerhymelabs.helenus.cassandra.migration;

import java.util.Properties;

public class MigrationConfiguration
{
	public static final String DEFAULT_SCRIPT_LOCATION = "/db/migrations";
	private static final String DEFAULT_METADATA_TABLE = "migration_meta";
	private static final String DEFAULT_METADATA_KEYSPACE = "migrations";

	private static final String SCRIPT_LOCATION = "migration.script_location";
	private static final String METADATA_TABLE = "migration.metadata_table";
	private static final String METADATA_KEYSPACE = "migration.metadata_keyspace";

	private String scriptLocation = DEFAULT_SCRIPT_LOCATION;
	private String metadataTable = DEFAULT_METADATA_TABLE;
	private String keyspace = DEFAULT_METADATA_KEYSPACE;

	public MigrationConfiguration()
	{
		super();
	}

	public MigrationConfiguration(Properties properties)
	{
		setScriptLocation(properties.getProperty(SCRIPT_LOCATION, DEFAULT_SCRIPT_LOCATION));
		setMetadataTable(properties.getProperty(METADATA_TABLE, DEFAULT_METADATA_TABLE));
	}

	public void setMetadataTable(String property)
	{
		this.metadataTable = property;
	}

	public String getMetadataTable()
	{
		return metadataTable;
	}

	public String getKeyspace()
	{
		return keyspace;
	}

	public void setKeyspace(String name)
	{
		this.keyspace = name;
	}

	public String getScriptLocation()
	{
		return scriptLocation;
	}

	public void setScriptLocation(String scriptLocation)
	{
		this.scriptLocation = scriptLocation;
	}
}
