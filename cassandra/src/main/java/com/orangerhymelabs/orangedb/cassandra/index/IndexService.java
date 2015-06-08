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
package com.orangerhymelabs.orangedb.cassandra.index;

import java.util.List;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.orangedb.cassandra.table.TableRepository;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class IndexService
{
	private IndexRepository indexes;
	private TableRepository tables;

	public IndexService(IndexRepository indexRepository, TableRepository tableRepository)
	{
		super();
		this.indexes = indexRepository;
		this.tables = tableRepository;
	}

	public Index create(Index index)
	{
		if (!tables.exists(index.table().getId()))
		{
			throw new ItemNotFoundException("Database not found: " + index.databaseName());
		}

		ValidationEngine.validateAndThrow(index);
		return indexes.create(index);
	}

	public void createAsync(Index table, FutureCallback<Index> callback)
	{
		tables.existsAsync(table.table().getId(), new FutureCallback<Boolean>()
			{
				@Override
                public void onSuccess(Boolean result)
                {
					if (!result)
					{
						callback.onFailure(new ItemNotFoundException("Database not found: " + table.databaseName()));
					}
					else
					{
						try
						{
							ValidationEngine.validateAndThrow(table);
							indexes.createAsync(table, callback);
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

	public Index read(String database, String table, String name)
	{
		return indexes.read(new Identifier(database, table, name));
	}

	public void readAsync(String database, String table, String name, FutureCallback<Index> callback)
	{
		indexes.readAsync(new Identifier(database, table, name), callback);
	}

	public List<Index> readAll(String database, String table)
	{
		Identifier id = new Identifier(database, table);
		if (!tables.exists(id))
		{
			throw new ItemNotFoundException("Table not found: " + Identifier.toSeparatedString(id, "_"));
		}

		return indexes.readAll(id);
	}

	public void readAllAsync(String database, String table, FutureCallback<List<Index>> callback)
	{
		Identifier id = new Identifier(database, table);
		tables.existsAsync(id, new FutureCallback<Boolean>()
			{
				@Override
                public void onSuccess(Boolean result)
                {
					if (!result)
					{
						callback.onFailure(new ItemNotFoundException("Table not found: " + Identifier.toSeparatedString(id, "_")));
					}
					else
					{
						indexes.readAllAsync(callback, database);
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

	public Index update(Index table)
	{
		ValidationEngine.validateAndThrow(table);
		return indexes.update(table);
	}

	public void updateAsync(Index table, FutureCallback<Index> callback)
	{
		try
		{
			ValidationEngine.validateAndThrow(table);
			indexes.updateAsync(table, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
	}

	public void delete(String database, String table, String name)
	{
		indexes.delete(new Identifier(database, table, name));
	}

	public void deleteAsync(String database, String table, String name, FutureCallback<Index> callback)
	{
		indexes.deleteAsync(new Identifier(database, table, name), callback);
	}
}
