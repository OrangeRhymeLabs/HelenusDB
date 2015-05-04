package com.orangerhymelabs.orangedb.database;

import com.orangerhymelabs.orangedb.event.AbstractEvent;

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
