package com.orangerhymelabs.orangedb.database;

import com.orangerhymelabs.orangedb.event.AbstractEvent;

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
