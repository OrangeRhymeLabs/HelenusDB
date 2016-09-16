/**
 * 
 */
package com.orangerhymelabs.helenus.cassandra.view.key;

import com.orangerhymelabs.helenus.cassandra.DataTypes;

/**
 * @author tfredrich
 * @since 1 Sept 2016
 */
public class ClusteringKeyComponent
extends KeyComponent
{
	public enum Ordering
	{
		ASC,
		DESC;

		public boolean isDescending()
		{
			return DESC.equals(this);
		}
	}

	private Ordering order;

	public ClusteringKeyComponent(String property, DataTypes type, Ordering order)
	{
		super(property, type);
		this.order = order;
	}

	public Ordering order()
	{
		return order;
	}
}
