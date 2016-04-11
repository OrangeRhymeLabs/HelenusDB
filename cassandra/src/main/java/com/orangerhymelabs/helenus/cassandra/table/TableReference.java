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
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;

import java.util.Objects;

/**
 * @author tfredrich
 * @since Jan 30, 2015
 */
public class TableReference
{
	@RegexValidation(name = "Database Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String database;

	@RegexValidation(name = "Table Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;

	@Required("ID Type")
	private DataTypes idType = DataTypes.UUID;

	public TableReference(String database, String table, DataTypes idType)
	{
		this.database = database;
		this.name = table;
		this.idType = idType;
	}

	public TableReference(Table table)
	{
		this(table.databaseName(), table.name(), table.idType());
	}

	public String database()
	{
		return database;
	}

	public String name()
	{
		return name;
	}

	public DataTypes idType()
	{
		return idType;
	}

	public Table toTable()
	{
		Table t = new Table();
		t.database(database);
		t.name(name);
		t.idType(idType);
		return t;
	}

	@Override
	public int hashCode()
	{
		int hash = 5;
		hash = 59 * hash + Objects.hashCode(this.database);
		hash = 59 * hash + Objects.hashCode(this.name);
		hash = 59 * hash + Objects.hashCode(this.idType);
		return hash;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}

		if (getClass() != obj.getClass())
		{
			return false;
		}

		final TableReference other = (TableReference) obj;

		if (!Objects.equals(this.database, other.database))
		{
			return false;
		}

		if (!Objects.equals(this.name, other.name))
		{
			return false;
		}

		if (this.idType != other.idType)
		{
			return false;
		}

		return true;
	}

	@Override
	public String toString()
	{
		return "TableReference{" + "database=" + database + ", name=" + name +  ", idType=" + idType.toString() + '}';
	}
}
