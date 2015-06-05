package com.orangerhymelabs.orangedb.persistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A base, abstract repository implementation that supports observation.
 * 
 * @author toddf
 * @since Oct 12, 2010
 */
public abstract class AbstractObservable<T>
implements Observable
{
	private List<Observer> observers = new ArrayList<Observer>();

	
	public AbstractObservable()
	{
		super();
	}


	@Override
	public void addObserver(Observer observer)
	{
		_observers().add(observer);
	}
	
	/**
	 * Remove all observers from this repository.
	 */
	@Override
	public void clearObservers()
	{
		_observers().clear();
	}
	
	private List<Observer> _observers()
	{
		return observers;
	}
	
	/**
	 * Returns the observers for this AbstractRepository as an unmodifiable list.
	 * 
	 * @return the repository's observers.
	 */
	@Override
	public List<Observer> observers()
	{
		return Collections.unmodifiableList(_observers());
	}
	
	@Override
	public boolean removeObserver(Observer observer)
	{
		return _observers().remove(observer);
	}

	@Override
	public void notify(ObservableState state, Object object)
	{
		for(Observer observer : _observers())
		{
			observer.observe(state, object);
		}
	}
}
