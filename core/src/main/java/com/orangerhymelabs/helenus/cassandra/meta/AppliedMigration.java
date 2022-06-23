package com.orangerhymelabs.helenus.cassandra.meta;

import java.util.Date;

import com.orangerhymelabs.helenus.cassandra.migration.Migration;

public class AppliedMigration
{
	private Migration migration;
	private String script;
	private String hash;
	private Date appliedAt = new Date();
	private long executionDurationMillis;
	private boolean wasSuccessful;

	public AppliedMigration(Migration migration, long executionDurationMillis, boolean wasSuccessful)
	{
		super();
		this.migration = migration;
		this.executionDurationMillis = executionDurationMillis;
		this.wasSuccessful = wasSuccessful;
		setScript(migration.getScript());
	}

	private void setScript(String script)
	{
		this.script = script;
		MD5 md5 = MD5.ofString(script);
		this.hash = md5.asBase64();
	}

	public int getVersion()
	{
		return migration.getVersion();
	}

	public String getDescription()
	{
		return migration.getDescription();
	}

	public String getScript()
	{
		return script;
	}

	public String getHash()
	{
		return hash;
	}

	public Date getAppliedAt()
	{
		return appliedAt;
	}

	public long getExecutionDurationMillis()
	{
		return executionDurationMillis;
	}

	public boolean wasSuccessful()
	{
		return wasSuccessful;
	}
}
