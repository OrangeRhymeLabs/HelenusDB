package com.orangerhymelabs.orangedb.cassandra.table;

import com.orangerhymelabs.orangedb.cassandra.event.AbstractEvent;
import com.orangerhymelabs.orangedb.persistence.Identifier;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class TableDeletedEvent
extends AbstractEvent<Identifier>
{
	public TableDeletedEvent(Identifier table)
	{
		super(table);
	}
}
