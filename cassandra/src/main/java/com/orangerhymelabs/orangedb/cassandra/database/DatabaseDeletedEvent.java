package com.orangerhymelabs.orangedb.cassandra.database;

import com.orangerhymelabs.orangedb.cassandra.event.AbstractEvent;
import com.orangerhymelabs.orangedb.persistence.Identifier;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class DatabaseDeletedEvent
extends AbstractEvent<Identifier>
{
	public DatabaseDeletedEvent(Identifier id)
	{
		super(id);
	}
}
