/*
    Copyright 2015, Strategic Gains, Inc.

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
package com.orangerhymelabs.helenus.cassandra.table;

import com.orangerhymelabs.helenus.cassandra.Constants;
import com.orangerhymelabs.helenus.persistence.AbstractEntity;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * Defines a materialized view of a Table.
 * 
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class View
extends AbstractEntity
{
	@Required("Table")
	@ChildValidation
	private TableReference table;

	@RegexValidation(name = "View Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;

	@Required("Key Definition")
	private String keys;

	// How long should the view's data live? (0 implies forever)
	private long ttl;

	public View()
	{
		super();
	}

	public boolean hasTable()
	{
		return (table != null);
	}

	public Table table()
	{
		return table.toTable();
	}

	public void table(Table table)
	{
		this.table = new TableReference(table);
	}

	public String databaseName()
	{
		return (hasTable() ? table.database() : null);
	}

	public String tableName()
	{
		return (hasTable() ? table.name() : null);
	}

	public boolean hasName()
	{
		return (name != null);
	}

	public String name()
	{
		return name;
	}

	public void name(String name)
	{
		this.name = name;
	}

	public boolean hasDescription()
	{
		return (description != null);
	}

	public String description()
	{
		return description;
	}

	public void description(String description)
	{
		this.description = description;
	}

	public String keys()
	{
		return keys;
	}

	public void keys(String keys)
	{
		this.keys = keys;
	}

	public long ttl()
	{
		return ttl;
	}

	public void ttl(long ttl)
	{
		this.ttl = ttl;
	}

	@Override
    public Identifier identifier()
    {
	    return (hasTable() & hasName() ? new Identifier(table.database(), table.name(), name) : null);
    }

	public String toDbTable()
	{
		return identifier().toDbName();
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder(name());

		if (hasDescription())
		{
			sb.append("=(");
			sb.append(description());
			sb.append(", Table=");
			sb.append(table().name());
			sb.append(", Keys=");
			sb.append(keys());
			sb.append(", TTL=");
			sb.append(ttl());
			sb.append(")");
		}
		return sb.toString();
	}
}
