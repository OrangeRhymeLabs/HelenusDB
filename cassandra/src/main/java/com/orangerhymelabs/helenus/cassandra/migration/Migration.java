package com.orangerhymelabs.helenus.cassandra.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

public class Migration
implements Comparable<Migration>
{
	private static final Logger LOG = LoggerFactory.getLogger(Migration.class);

	private String script;
	private int version;
	private String description;

	public int getVersion()
	{
		return version;
	}

	public void setVersion(int version)
	{
		this.version = version;
	}

	public String getDescription()
	{
		return description;
	}

	public void setDescription(String description)
	{
		this.description = description;
	}

	public boolean isApplicable(int from, int to)
	{
		return (getVersion() > from && getVersion() <= to);
	}

	public int compareTo(Migration that)
	{
		if (that == null) return 1;
		return that.getVersion() - this.getVersion();
	}

	public Migration()
	{
		super();
	}

	public String getScript()
	{
		return script;
	}

	public void setScript(String script)
	{
		this.script = script;
	}

	public boolean migrate(Session session)
	{
		try
		{
			String[] commands = getScript().split(";");

			for (String command : commands)
			{
				session.execute(command);
			}
		}
		catch(Throwable t)
		{
			LOG.error("Migration failed: " + getDescription(), t);
			return false;
		}

		return true;
	}
}
