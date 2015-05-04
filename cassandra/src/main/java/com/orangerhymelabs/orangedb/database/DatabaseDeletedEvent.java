package com.orangerhymelabs.orangedb.database;

import com.orangerhymelabs.orangedb.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DatabaseDeletedEvent
extends AbstractEvent<Database>
{
	public DatabaseDeletedEvent(Database database)
	{
		super(database);
	}
}
