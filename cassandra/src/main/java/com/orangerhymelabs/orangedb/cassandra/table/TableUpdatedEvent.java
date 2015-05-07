package com.orangerhymelabs.orangedb.cassandra.table;

import com.orangerhymelabs.orangedb.cassandra.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class TableUpdatedEvent
extends AbstractEvent<Table>
{
	public TableUpdatedEvent(Table table)
	{
		super(table);
	}
}
