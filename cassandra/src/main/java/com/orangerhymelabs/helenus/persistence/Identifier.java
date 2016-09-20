/*
    Copyright 2013, Strategic Gains, Inc.

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

		http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package com.orangerhymelabs.helenus.persistence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Supports the concept of a compound identifier. An Identifier is made up of components, which
 * are Object instances. The components are kept in order of which they are added.
 * 
 * @author toddf
 * @since Aug 29, 2013
 */
public class Identifier
implements Comparable<Identifier>
{
	private static final String DB_NAME_SEPARATOR = "_";
	private static final String TO_STRING_SEPARATOR = ", ";

	private List<Object> components = new ArrayList<Object>();

	/**
	 * Create an empty identifier.
	 */
	public Identifier()
	{
		super();
	}
	
	public Identifier(Identifier that)
	{
		this();
		
		if (that == null || that.isEmpty()) return;
		
		add(that.components.toArray());
	}

	/**
	 * Create an identifier with the given components. Duplicate instances
	 * are not added--only one instance of a component will exist in the identifier.
	 * 
	 * @param components
	 */
	public Identifier(Object... components)
	{
		this();
		add(components);
	}

	/**
	 * Add the given components, in order, to the identifier. Duplicate instances
	 * are not added--only one instance of a component will exist in the identifier.
	 * 
	 * @param components
	 */
	public Identifier add(Object... components)
    {
		if (components == null) return this;

		for (Object component : components)
		{
			add(component);
		}

		return this;
    }

	/**
	 * Add a single component to the identifier. The given component is added to
	 * the end of the identifier. Duplicate instances are not added--only one instance
	 * of a component will exist in the identifier.
	 * 
	 * @param component
	 */
	public Identifier add(Object component)
    {
		if (component == null) return this;

		components.add(component);
		return this;
    }

	/**
	 * Get an unmodifiable list of the components that make up this identifier.
	 * 
	 * @return an unmodifiable list of components.
	 */
	public List<Object> components()
	{
		return Collections.unmodifiableList(components);
	}

	/**
	 * Iterate the components of this identifier. Modifications to the underlying components
	 * are not possible via this iterator.
	 * 
	 * @return an iterator over the components of this identifier
	 */
	public Iterator<Object> iterator()
	{
		return components().iterator();
	}

	/**
	 * Indicates the number of components making up this identifier.
	 * 
	 * @return the number of components in this identifier.
	 */
	public int size()
	{
		return components.size();
	}

	/**
	 * Check for equality between identifiers. Returns true if the identifiers
	 * contain equal components. Otherwise, returns false.
	 * 
	 * @return true if the identifiers are equivalent.
	 */
	@Override
	public boolean equals(Object that)
	{
		return (compareTo((Identifier) that) == 0);
	}

	/**
	 * Returns a hash code for this identifier.
	 * 
	 * @return an integer hashcode
	 */
	@Override
	public int hashCode()
	{
		return 1 + components.hashCode();
	}

	/**
	 * Compares this identifer to another, returning -1, 0, or 1 depending
	 * on whether this identifier is less-than, equal-to, or greater-than
	 * the other identifier, respectively.
	 * 
	 * @return -1, 0, 1 to indicate less-than, equal-to, or greater-than, respectively.
	 */
    @SuppressWarnings({
        "unchecked", "rawtypes"
    })
    @Override
    public int compareTo(Identifier that)
    {
		if (that == null) return 1;
		if (this.size() < that.size()) return -1;
		if (this.size() > that.size()) return 1;

		int i = 0;
		int result = 0;

		while (result == 0 && i < size())
		{
			Object cThis = this.components.get(i);
			Object cThat = that.components.get(i);

			if (areComparable(cThis, cThat))
			{
				result = ((Comparable) cThis).compareTo(((Comparable) cThat));
			}
			else
			{
				result = (cThis.toString().compareTo(cThat.toString()));
			}
			
			++i;
		}

	    return result;
    }

	/**
     * Returns a string representation of this identifier.
     * 
     * @return a string representation of the identifier.
     */
	@Override
	public String toString()
	{
		if (components.isEmpty()) return "";

		return (components.size() == 1 ? primaryKey().toString() : "(" + Identifier.toSeparatedString(this, TO_STRING_SEPARATOR) + ")");
	}

	/**
	 * Returns the first component of the identifier. Return null if the identifier is empty.
	 * Equivalent to components().get(0).
	 * 
	 * @return the first component or null.
	 */
	public Object primaryKey()
	{
		return (isEmpty() ? null : components.get(0));
	}

	/**
	 * Return true if the identifier has no components.
	 * 
	 * @return true if the identifier is empty.
	 */
	public boolean isEmpty()
    {
	    return components.isEmpty();
    }

	public String toDbName()
	{
		return toSeparatedString(this, DB_NAME_SEPARATOR);
	}

	public static String toSeparatedString(Identifier id, String separator)
	{
		StringBuilder sb = new StringBuilder();
		Iterator<Object> iter = id.iterator();
		boolean isFirst = true;

		while(iter.hasNext())
		{
			if (!isFirst)
			{
				sb.append(separator);
			}
			else
			{
				isFirst = false;
			}

			sb.append(iter.next().toString());
		}

		return sb.toString();
	}

    private boolean areComparable(Object o1, Object o2)
    {
		if ((isComparable(o1) && isComparable(o2)) &&
			(o1.getClass().isAssignableFrom(o2.getClass()) ||
			o2.getClass().isAssignableFrom(o1.getClass())))
		{
			return true;
		}
	
	return false;
	}

    /**
	 * Returns true if the object implements Comparable.
	 * Otherwise, false.
	 * 
	 * @param object an instance
	 * @return true if the instance implements Comparable.
	 */
	private boolean isComparable(Object object)
    {
	    return (object instanceof Comparable);
    }

}
