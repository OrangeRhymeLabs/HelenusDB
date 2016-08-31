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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

/**
 * Caches all the DocumentRepository instances by table name. When a cache miss occurs, table is validated for existence.
 * 
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DocumentService
{
	//TODO: this should be a distributed cache, perhaps?
	//TODO: Must be invalidatable via events.
	private Map<String, DocumentRepository> repoCache = new HashMap<String, DocumentRepository>();
	private TableService tables;
	private DocumentRepositoryFactory factory;

	public DocumentService(TableService tableService, DocumentRepositoryFactory repositoryFactory)
	{
		super();
		this.tables = tableService;
		this.factory = repositoryFactory;
	}

	public ListenableFuture<Document> create(String database, String table, Document document)
	{
		ListenableFuture<DocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<DocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(DocumentRepository input)
			throws Exception
			{
				try
				{
					ValidationEngine.validateAndThrow(document);
					return input.create(document);
				}
				catch(ValidationException e)
				{
					return Futures.immediateFailedFuture(e);
				}
			}
		});
	}

	public void create(String database, String table, Document document, FutureCallback<Document> callback)
	{
		Futures.addCallback(create(database, table, document), callback);
	}

	public ListenableFuture<Document> read(String database, String table, Object id)
	{
		ListenableFuture<DocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<DocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(DocumentRepository input)
			throws Exception
			{
				return input.read(new Identifier(id));
			}
		});
	}

	public void read(String database, String table, Object id, FutureCallback<Document> callback)
	{
		Futures.addCallback(read(database, table, id), callback);
	}

	public ListenableFuture<List<Document>> readIn(String database, String table, Object... ids)
	{
		Identifier[] inIds = new Identifier[ids.length];
		int i = 0;

		for (Object id : ids)
		{
			inIds[i++] = new Identifier(id);
		}

		ListenableFuture<DocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<DocumentRepository, List<Document>>()
		{
			@Override
			public ListenableFuture<List<Document>> apply(DocumentRepository input)
			throws Exception
			{
				return input.readIn(inIds);
			}
		});
	}

	public void readIn(FutureCallback<List<Document>> callback, String database, String table, Object... ids)
	{
		Futures.addCallback(readIn(database, table, ids), callback);
	}

	public ListenableFuture<Document> update(String database, String table, Document document)
	{
		ListenableFuture<DocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<DocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(DocumentRepository input)
			throws Exception
			{
				try
				{
					ValidationEngine.validateAndThrow(document);
					return input.update(document);
				}
				catch(ValidationException e)
				{
					return Futures.immediateFailedFuture(e);
				}
			}
		});
	}

	public void update(String database, String table, Document document, FutureCallback<Document> callback)
    {
		Futures.addCallback(update(database, table, document), callback);
    }

	public ListenableFuture<Document> upsert(String database, String table, Document document)
	{
		ListenableFuture<DocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<DocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(DocumentRepository input)
			throws Exception
			{
				try
				{
					ValidationEngine.validateAndThrow(document);
					return input.upsert(document);
				}
				catch(ValidationException e)
				{
					return Futures.immediateFailedFuture(e);
				}
			}
		});
	}

	public void upsert(String database, String table, Document document, FutureCallback<Document> callback)
    {
		Futures.addCallback(upsert(database, table, document), callback);
    }

	public ListenableFuture<Boolean> delete(String database, String table, Object id)
	{
		ListenableFuture<DocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<DocumentRepository, Boolean>()
		{
			@Override
			public ListenableFuture<Boolean> apply(DocumentRepository input)
			throws Exception
			{
				return input.delete(new Identifier(id));
			}
		});
	}

	public void delete(String database, String table, Object id, FutureCallback<Boolean> callback)
	{
		Futures.addCallback(delete(database, table, id), callback);
	}

	public ListenableFuture<Boolean> exists(String database, String table, Object id)
	{
		ListenableFuture<DocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<DocumentRepository, Boolean>()
		{
			@Override
			public ListenableFuture<Boolean> apply(DocumentRepository input)
			throws Exception
			{
				return input.exists(new Identifier(id));
			}
		});
	}

	public void exists(String database, String table, Object id, FutureCallback<Boolean> callback)
	{
		Futures.addCallback(exists(database, table, id), callback);
	}

	private ListenableFuture<DocumentRepository> acquireRepositoryFor(String database, String table)
    {
		String cacheKey = String.format("%s_%s", database, table);
		DocumentRepository repo = repoCache.get(cacheKey);

		if (repo != null)
		{
			return Futures.immediateFuture(repo);
		}

		ListenableFuture<Table> futureTable = tables.read(database, table);
		return Futures.transformAsync(futureTable, new AsyncFunction<Table, DocumentRepository>()
		{
			@Override
			public ListenableFuture<DocumentRepository> apply(Table input)
			throws Exception
			{
				DocumentRepository repo = factory.newInstance(input);
				repoCache.put(cacheKey, repo);
				return Futures.immediateFuture(repo);
			}
		});
    }
}
