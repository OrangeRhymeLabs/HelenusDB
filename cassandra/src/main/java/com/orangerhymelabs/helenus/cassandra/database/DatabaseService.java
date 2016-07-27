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
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;
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
		try
		{
			ValidationEngine.validateAndThrow(database);
			return databases.create(database).get();
		}
		catch (ExecutionException e)
		{
			throw (RuntimeException) e.getCause();
		}
		catch (InterruptedException e)
		{
			throw new StorageException(e);
		}
	}

	public void create(Database database, FutureCallback<Database> callback)
	{
		try
		{
			ValidationEngine.validateAndThrow(database);
			Futures.addCallback(databases.create(database), callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
	}

	public Database read(String name)
	{
		try
		{
			return databases.read(new Identifier(name)).get();
		}
		catch(ExecutionException e)
		{
			throw (RuntimeException) e.getCause();
		}
		catch (InterruptedException e)
		{
			throw new StorageException(e);
		}		
	}

	public void read(String name, FutureCallback<Database> callback)
	{
		Futures.addCallback(databases.read(new Identifier(name)), callback);
	}

	public List<Database> readAll(Object... parms)
	{
		try
		{
			return databases.readAll(parms).get();
		}
		catch(ExecutionException e)
		{
			throw (RuntimeException) e.getCause();
		}
		catch (InterruptedException e)
		{
			throw new StorageException(e);
		}
	}

	public void readAll(FutureCallback<List<Database>> callback, Object... parms)
	{
		Futures.addCallback(databases.readAll(parms), callback);
	}

	public Database update(Database database)
	{
		try
		{
			ValidationEngine.validateAndThrow(database);
			return databases.update(database).get();
		}
		catch (ExecutionException e)
		{
			throw (RuntimeException) e.getCause();
		}
		catch (InterruptedException e)
		{
			throw new StorageException(e);
		}
	}

	public void update(Database database, FutureCallback<Database> callback)
    {
		try
		{
			ValidationEngine.validateAndThrow(database);
			Futures.addCallback(databases.update(database), callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
    }

	public boolean delete(String name)
	{
		try
		{
			return databases.delete(new Identifier(name)).get();
		}
		catch (ExecutionException e)
		{
			throw (RuntimeException) e.getCause();
		}
		catch (InterruptedException e)
		{
			throw new StorageException(e);
		}
	}

	public void delete(String name, FutureCallback<Boolean> callback)
    {
		Futures.addCallback(databases.delete(new Identifier(name)), callback);
    }
}
