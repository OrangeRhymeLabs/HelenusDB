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
package com.orangerhymelabs.orangedb.cassandra.database;

import java.util.Objects;

import com.orangerhymelabs.orangedb.cassandra.Constants;
import com.orangerhymelabs.orangedb.persistence.AbstractEntity;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.annotation.RegexValidation;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class Database
extends AbstractEntity
{
	@RegexValidation(name = "Database Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
	private String name;
	private String description;

	public Database()
	{
		super();
	}

	public Database(String name)
	{
		this();
		name(name);
	}

	public Identifier getIdentifier()
	{
		return new Identifier(name());
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

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder(name);

		if (hasDescription())
		{
			sb.append(" (");
			sb.append(description);
			sb.append(")");
		}
		return sb.toString();
	}

	@Override
	public int hashCode()
	{
		int hash = 5;
		hash = 59 * hash + Objects.hashCode(this.name);
		hash = 59 * hash + Objects.hashCode(this.description);
		return hash;
	}

	@Override
	public boolean equals(Object object)
	{
		if (object == null)
		{
			return false;
		}

		if (getClass() != object.getClass())
		{
			return false;
		}

		final Database that = (Database) object;
		if (!Objects.equals(this.name, that.name))
		{
			return false;
		}

		if (!Objects.equals(this.description, that.description))
		{
			return false;
		}

		return super.equals(object);
	}
}
