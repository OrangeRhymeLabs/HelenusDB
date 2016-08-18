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
import java.util.concurrent.ExecutionException;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseRepository;
import com.orangerhymelabs.helenus.cassandra.database.DatabaseService;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.exception.StorageException;
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

	public Table create(Table table)
	{
		databases.exists(table.database().getIdentifier(), new FutureCallback<Boolean>()
		{
			@Override
			public void onSuccess(Boolean result)
			{
				
			}

			@Override
			public void onFailure(Throwable t)
			{
			}
		});

		Futures.transform(databases.exists(table.database().getIdentifier()), new Function<Boolean, Table>()
		{
			@Override
			public Table apply(Boolean input)
			{
				if (input)
				{
					ValidationEngine.validateAndThrow(table);
					return tables.create(table).get();
				}
				// TODO Auto-generated method stub
				return null;
			}
		});
		if (!databases.exists(table.database().getIdentifier()).get())
		{
			throw new ItemNotFoundException("Database not found: " + table.databaseName());
		}

		ValidationEngine.validateAndThrow(table);
		return tables.create(table).get();
	}

	public void create(Table table, FutureCallback<Table> callback)
	{
		databases.exists(table.database().getIdentifier(), new FutureCallback<Boolean>()
			{
				@Override
                public void onSuccess(Boolean successful)
                {
					if (!successful)
					{
						callback.onFailure(new ItemNotFoundException("Database not found: " + table.databaseName()));
					}
					else
					{
						try
						{
							ValidationEngine.validateAndThrow(table);
							tables.create(table, callback);
						}
						catch(ValidationException e)
						{
							callback.onFailure(e);
						}
					}
                }

				@Override
                public void onFailure(Throwable t)
                {
					callback.onFailure(t);
                }
			}
		);
	}

	public Table read(String database, String table)
	{
		return tables.read(new Identifier(database, table));
	}

	public void readAsync(String database, String table, FutureCallback<Table> callback)
	{
		tables.readAsync(new Identifier(database, table), callback);
	}

	public List<Table> readAll(String database)
	{
		Identifier id = new Identifier(database);
		if (!databases.exists(id))
		{
			throw new ItemNotFoundException("Database not found: " + database);
		}

		return tables.readAll(id);
	}

	public void readAllAsync(String database, FutureCallback<List<Table>> callback)
	{
		databases.existsAsync(new Identifier(database), new FutureCallback<Boolean>()
			{
				@Override
                public void onSuccess(Boolean result)
                {
					if (!result)
					{
						callback.onFailure(new ItemNotFoundException("Database not found: " + database));
					}
					else
					{
						tables.readAllAsync(callback, database);
					}
                }

				@Override
                public void onFailure(Throwable t)
                {
					callback.onFailure(t);
                }
			}
		);
	}

	public Table update(Table table)
	{
		try
		{
			ValidationEngine.validateAndThrow(table);
			return tables.update(table).get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new StorageException(e);
		}
	}

	public void updateAsync(Table table, FutureCallback<Table> callback)
	{
		try
		{
			ValidationEngine.validateAndThrow(table);
			tables.updateAsync(table, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
	}

	public void delete(String database, String table)
	{
		tables.delete(new Identifier(database, table));
	}

	public void deleteAsync(String database, String table, FutureCallback<Table> callback)
	{
		tables.deleteAsync(new Identifier(database, table), callback);
	}
}
