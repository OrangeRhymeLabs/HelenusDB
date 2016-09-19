/**
 * 
 */
package com.orangerhymelabs.helenus.cassandra.table.key;

import com.orangerhymelabs.helenus.cassandra.DataTypes;

/**
 * @author tfredrich
 * @since 1 Sept 2016
 */
public class KeyComponent
{
	private String property;
	private DataTypes type;

	public KeyComponent(String property, DataTypes type)
	{
		super();
		this.property = property;
		this.type = type;
	}

	public String property()
	{
		return property;
	}

	public DataTypes type()
	{
		return type;
	}
}
