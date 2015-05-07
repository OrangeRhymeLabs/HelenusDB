package com.orangerhymelabs.orangedb.cassandra.database;

import com.orangerhymelabs.orangedb.cassandra.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DatabaseUpdatedEvent
extends AbstractEvent<Database>
{
	public DatabaseUpdatedEvent(Database database)
	{
		super(database);
	}
}
