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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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

	public void exists(String name, FutureCallback<Boolean> callback)
	{
		Futures.addCallback(exists(name), callback);
	}

	public ListenableFuture<Boolean> exists(String name)
	{
		return databases.exists(new Identifier(name));
	}

	public void create(Database database, FutureCallback<Database> callback)
	{
		Futures.addCallback(create(database), callback);
	}

	public ListenableFuture<Database> create(Database database)
	{
		try
		{
			ValidationEngine.validateAndThrow(database);
			return databases.create(database);
		}
		catch(ValidationException e)
		{
			return Futures.immediateFailedFuture(e);
		}
	}

	public void read(String name, FutureCallback<Database> callback)
	{
		Futures.addCallback(read(name), callback);
	}

	private ListenableFuture<Database> read(String name)
	{
		return databases.read(new Identifier(name));
	}

	public void readAll(FutureCallback<List<Database>> callback, Object... parms)
	{
		Futures.addCallback(readAll(parms), callback);
	}

	private ListenableFuture<List<Database>> readAll(Object[] parms)
	{
		return databases.readAll(parms);
	}

	public void update(Database database, FutureCallback<Database> callback)
    {
		Futures.addCallback(update(database), callback);
    }

	private ListenableFuture<Database> update(Database database)
	{
		try
		{
			ValidationEngine.validateAndThrow(database);
			return databases.update(database);
		}
		catch(ValidationException e)
		{
			return Futures.immediateFailedFuture(e);
		}
	}

	public void delete(String name, FutureCallback<Boolean> callback)
    {
		Futures.addCallback(delete(name), callback);
    }

	private ListenableFuture<Boolean> delete(String name)
	{
		return databases.delete(new Identifier(name));
	}
}
