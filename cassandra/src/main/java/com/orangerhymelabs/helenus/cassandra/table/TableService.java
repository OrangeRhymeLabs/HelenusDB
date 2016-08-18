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

import java.util.List;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseService;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class TableService
{
	private TableRepository tables;
	private DatabaseService databases;

	public TableService(DatabaseService databaseService, TableRepository tableRepository)
	{
		super();
		this.databases = databaseService;
		this.tables = tableRepository;
	}

	public void create(Table table, FutureCallback<Table> callback)
	{
		Futures.addCallback(create(table), callback);
	}

	public ListenableFuture<Table> create(Table table)
	{
		ListenableFuture<Boolean> dbFuture = databases.exists(table.database().getIdentifier());
		return Futures.transformAsync(dbFuture, new AsyncFunction<Boolean, Table>()
		{
			@Override
			public ListenableFuture<Table> apply(Boolean exists)
			throws Exception
			{
				if (!exists)
				{
					try
					{
						ValidationEngine.validateAndThrow(table);
						return tables.create(table);
					}
					catch(ValidationException e)
					{
						return Futures.immediateFailedFuture(e);
					}
				}
				else
				{
					return Futures.immediateFailedFuture(new ItemNotFoundException("Database not found: " + table.databaseName()));
				}
			}
		});
	}

	public void read(String database, String table, FutureCallback<Table> callback)
	{
		Futures.addCallback(read(database, table), callback);
	}

	public ListenableFuture<Table> read(String database, String table)
	{
		return tables.read(new Identifier(database, table));
	}

	public ListenableFuture<List<Table>> readAll(String database, Object... parms)
	{
		ListenableFuture<Boolean> dbFuture = databases.exists(new Identifier(database));
		return Futures.transformAsync(dbFuture, new AsyncFunction<Boolean, List<Table>>()
		{
			@Override
			public ListenableFuture<List<Table>> apply(Boolean exists)
			throws Exception
			{
				if (exists)
				{
					return tables.readAll(parms);
				}
				else
				{
					return Futures.immediateFailedFuture(new ItemNotFoundException("Database not found: " + database));
				}
			}
		});
	}

	public void readAll(String database, FutureCallback<List<Table>> callback)
	{
		Futures.addCallback(readAll(database), callback);
	}

	public ListenableFuture<Table> update(Table table)
	{
		ListenableFuture<Boolean> dbFuture = databases.exists(table.database().getIdentifier());
		return Futures.transformAsync(dbFuture, new AsyncFunction<Boolean, Table>()
		{
			@Override
			public ListenableFuture<Table> apply(Boolean exists)
			throws Exception
			{
				if (!exists)
				{
					try
					{
						ValidationEngine.validateAndThrow(table);
						return tables.update(table);
					}
					catch(ValidationException e)
					{
						return Futures.immediateFailedFuture(e);
					}
				}
				else
				{
					return Futures.immediateFailedFuture(new ItemNotFoundException("Database not found: " + table.databaseName()));
				}
			}
		});
	}

	public void update(Table table, FutureCallback<Table> callback)
	{
		Futures.addCallback(update(table), callback);
	}

	public ListenableFuture<Boolean> delete(String database, String table)
	{
		return tables.delete(new Identifier(database, table));
	}

	public void delete(String database, String table, FutureCallback<Boolean> callback)
	{
		Futures.addCallback(delete(database, table), callback);
	}
}
