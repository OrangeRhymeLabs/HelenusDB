package com.orangerhymelabs.orangedb.cassandra.event;


public interface EventFactory
{
	Object newCreatedEvent(Object object);
	Object newUpdatedEvent(Object object);
	Object newDeletedEvent(Object object);
}
