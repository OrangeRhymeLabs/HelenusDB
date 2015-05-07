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
implements Observable<T>
{
	// SECTION: INSTANCE VARIABLES
	
	private List<Observer<T>> observers = new ArrayList<Observer<T>>();

	
	// SECTION: CONSTRUCTORS
	
	public AbstractObservable()
	{
		super();
	}


	// SECTION: ACCESSORS/MUTATORS

	public void addObserver(Observer<T> observer)
	{
		_observers().add(observer);
	}
	
	/**
	 * Remove all observers from this repository.
	 */
	public void clearObservers()
	{
		_observers().clear();
	}
	
	private List<Observer<T>> _observers()
	{
		return observers;
	}
	
	/**
	 * Returns the observers for this AbstractRepository as an unmodifiable list.
	 * 
	 * @return the repository's observers.
	 */
	public List<Observer<T>> observers()
	{
		return Collections.unmodifiableList(_observers());
	}
	
	public boolean removeObserver(Observer<T> observer)
	{
		return _observers().remove(observer);
	}

	public void notify(T object)
	{
		for(Observer<T> observer : _observers())
		{
			observer.observe(object);
		}
	}
}
