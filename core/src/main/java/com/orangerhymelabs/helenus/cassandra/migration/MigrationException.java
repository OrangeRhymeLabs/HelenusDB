package com.orangerhymelabs.helenus.cassandra.migration;

public class MigrationException
extends RuntimeException
{
	private static final long serialVersionUID = -832550927700914427L;

	public MigrationException()
	{
	}

	public MigrationException(String message)
	{
		super(message);
	}

	public MigrationException(Throwable cause)
	{
		super(cause);
	}

	public MigrationException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public MigrationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
