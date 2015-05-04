package com.orangerhymelabs.orangedb.table;

import com.orangerhymelabs.orangedb.event.AbstractEvent;

/**
 * @author toddf
 * @since Nov 19, 2014
 */
public class TableCreatedEvent
extends AbstractEvent<Table>
{
	public TableCreatedEvent(Table table)
	{
		super(table);
	}
}
