/**
 * 
 */
package com.orangerhymelabs.helenus.cassandra.table.key;

/**
 * @author tfredrich
 * @since 2 Sept 2016
 */
public class KeyDefinitionException
extends Exception
{
	private static final long serialVersionUID = 2488298881183539211L;

	public KeyDefinitionException()
	{
		super();
	}

	public KeyDefinitionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public KeyDefinitionException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public KeyDefinitionException(String message)
	{
		super(message);
	}

	public KeyDefinitionException(Throwable cause)
	{
		super(cause);
	}
}
