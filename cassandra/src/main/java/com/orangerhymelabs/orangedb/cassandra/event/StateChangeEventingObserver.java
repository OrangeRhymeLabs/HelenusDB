package com.orangerhymelabs.orangedb.cassandra.event;

import com.orangerhymelabs.orangedb.persistence.ObservableState;
import com.orangerhymelabs.orangedb.persistence.Observer;
import com.strategicgains.eventing.DomainEvents;

/**
 * @author toddf
 * @since Aug 1, 2014
 */
public class StateChangeEventingObserver<T>
implements Observer<T>
{
	private EventFactory<T> factory;

	public StateChangeEventingObserver(EventFactory<T> eventFactory)
	{
		super();
		this.factory = eventFactory;
	}

	@Override
    public void observe(ObservableState state, T object)
    {
		if (factory != null)
		{
			switch(state)
			{
				case AFTER_CREATE:
					publish(factory.newCreatedEvent(object));
					break;
				case AFTER_DELETE:
					publish(factory.newDeletedEvent(object));
					break;
				case AFTER_UPDATE:
					publish(factory.newUpdatedEvent(object));
					break;
				default:
					return;
			}
		}
    }

	private void publish(Object event)
    {
		if (event == null) return;

		DomainEvents.publish(event);
    }
}
