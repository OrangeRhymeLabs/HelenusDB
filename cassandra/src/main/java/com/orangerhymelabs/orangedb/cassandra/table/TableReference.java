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
import com.strategicgains.syntaxe.annotation.RegexValidation;

import java.util.Objects;

/**
 * @author toddf
 * @since Jan 30, 2015
 */
public class TableReference
{

	@RegexValidation(name = "Database Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String database;

	@RegexValidation(name = "Table Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;

	public TableReference(String database, String table)
	{
		this.database = database;
		this.name = table;
	}

	public TableReference(Table table)
	{
		this(table.databaseName(), table.name());
	}

	public String database()
	{
		return database;
	}

	public String name()
	{
		return name;
	}

	public Table asObject()
	{
		Table t = new Table();
		t.database(database);
		t.name(name);
		return t;
	}

	@Override
	public int hashCode()
	{
		int hash = 5;
		hash = 59 * hash + Objects.hashCode(this.database);
		hash = 59 * hash + Objects.hashCode(this.name);
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

		return true;
	}

	@Override
	public String toString()
	{
		return "TableReference{" + "database=" + database + ", name=" + name + '}';
	}
}
