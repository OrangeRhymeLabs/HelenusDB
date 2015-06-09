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
package com.orangerhymelabs.orangedb.cassandra.table;

import com.orangerhymelabs.orangedb.cassandra.Constants;
import com.orangerhymelabs.orangedb.cassandra.database.Database;
import com.orangerhymelabs.orangedb.cassandra.database.DatabaseReference;
import com.orangerhymelabs.orangedb.cassandra.index.FieldType;
import com.orangerhymelabs.orangedb.persistence.AbstractEntity;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class Table
extends AbstractEntity
{
	@Required("Database")
	@ChildValidation
	private DatabaseReference database;

	@RegexValidation(name = "Table Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;
	private String schema;

	@Required("Table Type")
	private TableType type = TableType.DOCUMENT;

	@Required("ID Type")
	private FieldType idType = FieldType.UUID;

	// How long should the table's data live?
	private long ttl;

	public Table()
	{
		super();
	}

	public boolean hasDatabase()
	{
		return (database != null);
	}

	public Database database()
	{
		return database.asObject();
	}

	public void database(Database database)
	{
		this.database = new DatabaseReference(database);
	}

	public void database(String name)
    {
		this.database = new DatabaseReference(name);
    }

	public String databaseName()
    {
		return (hasDatabase() ? database.name() : null);
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

	public TableType type()
	{
		return type;
	}

	public void type(TableType type)
	{
		this.type = type;
	}

	public String schema()
	{
		return schema;
	}

	public boolean hasSchema()
	{
		return (schema != null);
	}

	public void schema(String schema)
	{
		this.schema = schema;
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
    public Identifier getId()
    {
	    return (hasDatabase() & hasName() ? new Identifier(database.name(), name) : null);
    }

	public FieldType idType()
	{
		return idType;
	}

	public void idType(FieldType idType)
	{
		this.idType = idType;
	}

	public String toDbTable()
	{
		return getId().toDbName();
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder(name());

		if (hasDescription())
		{
			sb.append("=(");
			sb.append(description());
			sb.append(", Type=");
			sb.append(type());
			sb.append(", ID Type=");
			sb.append(idType());
			sb.append(", TTL=");
			sb.append(ttl());
			sb.append(")");
		}
		return sb.toString();
	}
}
