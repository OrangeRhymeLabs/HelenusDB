package com.orangerhymelabs.orangedb.cassandra.database;

import com.orangerhymelabs.orangedb.cassandra.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DatabaseCreatedEvent
extends AbstractEvent<Database>
{
	public DatabaseCreatedEvent(Database database)
	{
		super(database);
	}
}
