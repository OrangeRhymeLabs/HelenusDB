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
package com.orangerhymelabs.helenus.cassandra.document;

import java.sql.ResultSet;
import java.util.function.Function;

import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.Repository;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinition;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionParser;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * Document repositories are unique per document/table and therefore must be cached by table.
 * 
 * @author tfredrich
 * @since Jun 8, 2015
 */
public abstract class AbstractDocumentRepository
implements Repository<Document>
{
	private String tableName;
	private KeyDefinition keyDefinition;

	protected AbstractDocumentRepository(String keyspace, String tableName, String keys)
	throws KeyDefinitionException
	{
		this.keyDefinition = new KeyDefinitionParser().parse(keys);
		this.tableName = tableName;
	}

	public boolean exists(Identifier id)
	{
		ListenableFuture<ResultSet> future = submitExists(id);
		return Futures.transform(future, new Function<ResultSet, Boolean>()
		{
			@Override
			public Boolean apply(ResultSet result)
			{
				return result.one().getLong(0) > 0;
			}
		}, MoreExecutors.directExecutor());
	}

	public Document upsert(Document entity)
	{
		ListenableFuture<ResultSet> future = submitUpsert(entity);
		return Futures.transformAsync(future, new AsyncFunction<ResultSet, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(ResultSet result)
			throws Exception
			{
				if (result.wasApplied())
				{
					return Futures.immediateFuture(entity);
				}

				//TODO: This doesn't provide any informational value... what should it be?
				return Futures.immediateFailedFuture(new StorageException(String.format("Table %s failed to store document: %s", tableName, entity.toString())));
			}
		}, MoreExecutors.directExecutor());
	}

	private Identifier marshalId(KeyDefinition keyDefinition, Row row)
	{
		Identifier id = new Identifier();
		keyDefinition.components().forEach(t -> id.add(IdPropertyConverter.marshal(t.property(), t.type(), row)));
		return id;
	}
}
