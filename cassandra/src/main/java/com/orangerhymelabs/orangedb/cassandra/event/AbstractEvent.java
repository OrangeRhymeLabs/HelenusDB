package com.orangerhymelabs.orangedb.cassandra.event;

public abstract class AbstractEvent<T>
extends StateChangeEvent<T>
{
	public AbstractEvent(T data)
    {
	    super(data);
    }
}
