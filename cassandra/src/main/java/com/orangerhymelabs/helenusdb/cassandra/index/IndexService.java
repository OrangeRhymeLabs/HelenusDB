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
package com.orangerhymelabs.helenusdb.cassandra.index;

import java.util.List;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.helenusdb.cassandra.table.Table;
import com.orangerhymelabs.helenusdb.cassandra.table.TableRepository;
import com.orangerhymelabs.helenusdb.exception.DuplicateItemException;
import com.orangerhymelabs.helenusdb.persistence.Identifier;
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
		Table previous = tables.read(index.table().getIdentifier());
		index.table(previous);
		ValidationEngine.validateAndThrow(index);
		allowOnlyOneLuceneIndex(index, previous);
		return indexes.create(index);
	}

	public void createAsync(Index index, FutureCallback<Index> callback)
	{
		tables.readAsync(index.table().getIdentifier(), new FutureCallback<Table>()
			{
				@Override
                public void onSuccess(Table previous)
                {
					try
					{
						index.table(previous);
						ValidationEngine.validateAndThrow(index);
					}
					catch(ValidationException e)
					{
						callback.onFailure(e);
					}

					try
					{
						allowOnlyOneLuceneIndex(index, previous);
					}
					catch(DuplicateItemException e)
					{
						callback.onFailure(e);
						return;
					}

					indexes.createAsync(index, callback);
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
		return indexes.readAll(new Identifier(database, table));
	}

	public void readAllAsync(String database, String table, FutureCallback<List<Index>> callback)
	{
		indexes.readAllAsync(callback, new Identifier(database, table));
	}

	public Index update(Index index)
	{
		Table previous = tables.read(index.table().getIdentifier());
		index.table(previous);
		ValidationEngine.validateAndThrow(index);
		return indexes.update(index);
	}

	public void updateAsync(Index index, FutureCallback<Index> callback)
	{
		tables.readAsync(index.table().getIdentifier(), new FutureCallback<Table>()
			{
				@Override
                public void onSuccess(Table previous)
                {
					try
					{
						index.table(previous);
						ValidationEngine.validateAndThrow(index);
						indexes.updateAsync(index, callback);
					}
					catch(ValidationException e)
					{
						callback.onFailure(e);
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

	public void delete(String database, String table, String name)
	{
		indexes.delete(new Identifier(database, table, name));
	}

	public void deleteAsync(String database, String table, String name, FutureCallback<Index> callback)
	{
		indexes.deleteAsync(new Identifier(database, table, name), callback);
	}

	/**
	 * Ensure no other Lucene indexes exist on this table. If one already exists,
	 * throw DuplicateItemException.
	 * 
	 * Note: this method is VERY expensive since it reads all the indexes for a given table,
	 * looping through them to determine if there's already a lucene index present.
	 * 
	 * @param index
	 * @param previous
	 * @throws DuplicateItemException
	 */
	private void allowOnlyOneLuceneIndex(Index index, Table previous)
	{
		if (index.isLucene())
		{
			for (Index ndx : indexes.readFor(previous.databaseName(), previous.name()))
			{
				if (ndx.isLucene())
				{
					throw new DuplicateItemException("Lucene index already exists on this table: " + ndx.name());
				}
			}
		}
	}
}
