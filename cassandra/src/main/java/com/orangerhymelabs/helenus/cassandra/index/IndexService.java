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
package com.orangerhymelabs.helenus.cassandra.index;

import java.util.List;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableRepository;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;
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
		ensureUniqueExternalIndex(index, previous);
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
						ensureUniqueExternalIndex(index, previous);
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
	 * Ensure no other external indexes exist of this same type on this table. If one already exists,
	 * throw DuplicateItemException.
	 * 
	 * Note: this method is VERY expensive since it reads all the indexes for a given table,
	 * looping through them to determine if there's already an external index matching the external indexer.
	 * 
	 * @param index the proposed new index.
	 * @param table the table being considered for indexing.
	 * @throws DuplicateItemException if the proposed index type is external (Lucene, SOLR, ElasticSearch, etc.) and one already exists on the table.
	 */
	private void ensureUniqueExternalIndex(Index index, Table table)
	{
		if (index.isExternal())
		{
			indexes.readForAsync(table.databaseName(), table.name(), new FutureCallback<List<Index>>()
			{
				@Override
				public void onSuccess(List<Index> results)
				{
					for (Index ndx : results)
					{
						if (index.engine().equals(ndx.engine()))
						{
							throw new DuplicateItemException(index.engine().name() + " index already exists on this table: " + ndx.name());
						}
					}
				}

				@Override
				public void onFailure(Throwable t)
				{
					throw new StorageException(t);
				}
			});
		}
	}
}
