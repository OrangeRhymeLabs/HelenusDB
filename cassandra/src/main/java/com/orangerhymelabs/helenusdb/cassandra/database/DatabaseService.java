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
package com.orangerhymelabs.helenusdb.cassandra.database;

import java.util.List;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.helenusdb.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

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

	public Database create(Database database)
	{
		ValidationEngine.validateAndThrow(database);
		return databases.create(database);
	}

	public void createAsync(Database database, FutureCallback<Database> callback)
	{
		try
		{
			ValidationEngine.validateAndThrow(database);
			databases.createAsync(database, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
	}

	public Database read(String name)
	{
		return databases.read(new Identifier(name));		
	}

	public void readAsync(String name, FutureCallback<Database> callback)
	{
		databases.readAsync(new Identifier(name), callback);
	}

	public List<Database> readAll()
	{
		return databases.readAll();
	}

	public void readAllAsync(FutureCallback<List<Database>> callback)
	{
		databases.readAllAsync(callback);
	}

	public Database update(Database database)
	{
		ValidationEngine.validateAndThrow(database);
		return databases.update(database);
	}

	public void updateAsync(Database database, FutureCallback<Database> callback)
    {
		try
		{
			ValidationEngine.validateAndThrow(database);
			databases.updateAsync(database, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
    }

	public void delete(String name)
	{
		databases.delete(new Identifier(name));
	}

	public void deleteAsync(String name, FutureCallback<Database> callback)
    {
		databases.deleteAsync(new Identifier(name), callback);
    }
}
