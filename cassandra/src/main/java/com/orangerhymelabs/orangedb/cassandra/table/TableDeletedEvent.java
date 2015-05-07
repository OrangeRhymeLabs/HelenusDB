package com.orangerhymelabs.orangedb.cassandra.table;

import com.orangerhymelabs.orangedb.cassandra.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class TableDeletedEvent
extends AbstractEvent<Table>
{
	public TableDeletedEvent(Table table)
	{
		super(table);
	}
}
