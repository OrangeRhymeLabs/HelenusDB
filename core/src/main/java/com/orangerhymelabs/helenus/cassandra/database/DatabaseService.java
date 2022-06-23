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
package com.orangerhymelabs.helenus.cassandra.database;

import java.util.List;

import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DatabaseService
{
	private DatabaseRepository databases;
	
	public DatabaseService(DatabaseRepository databaseRepository)
	{
		super();
		this.databases = databaseRepository;
	}

	public boolean exists(String name)
	{
		return databases.exists(new Identifier(name));
	}

	public Database create(Database database)
	{
		ValidationEngine.validateAndThrow(database);
		return databases.create(database);
	}

	public Database read(String name)
	{
		return databases.read(new Identifier(name));
	}

	public List<Database> readAll(Object[] parms)
	{
		return databases.readAll(parms);
	}

	public Database update(Database database)
	{
		ValidationEngine.validateAndThrow(database);
		return databases.update(database);
	}

	public boolean delete(String name)
	{
		return databases.delete(new Identifier(name));
	}
}
