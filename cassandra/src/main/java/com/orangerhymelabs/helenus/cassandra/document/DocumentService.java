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
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;

/**
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
		ValidationEngine.validateAndThrow(document);
		DocumentRepository docs = acquireRepositoryFor(database, table);
		return docs.create(document);
	}

	public void create(String database, String table, Document document, FutureCallback<Document> callback)
	{
		Futures.addCallback(create(database, table, document), callback);
	}

	public ListenableFuture<Document> read(String database, String table, Object id)
	{
		DocumentRepository docs = acquireRepositoryFor(database, table);
		return docs.read(new Identifier(id));
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

		DocumentRepository docs = acquireRepositoryFor(database, table);
		return docs.readIn(inIds);
	}

	public void readIn(FutureCallback<List<Document>> callback, String database, String table, Object... ids)
	{
		Futures.addCallback(readIn(database, table, ids), callback);
	}

	public ListenableFuture<Document> update(String database, String table, Document document)
	{
		ValidationEngine.validateAndThrow(document);
		DocumentRepository docs = acquireRepositoryFor(database, table);
		return docs.update(document);
	}

	public void update(String database, String table, Document document, FutureCallback<Document> callback)
    {
		Futures.addCallback(update(database, table, document), callback);
    }

	public ListenableFuture<Boolean> delete(String database, String table, Object id)
	{
		DocumentRepository docs = acquireRepositoryFor(database, table);
		return docs.delete(new Identifier(id));
	}

	public void delete(String database, String table, Object id, FutureCallback<Boolean> callback)
	{
		Futures.addCallback(delete(database, table, id), callback);
	}

	private DocumentRepository acquireRepositoryFor(String database, String table)
    {
		String cacheKey = String.format("%s_%s", database, table);
		DocumentRepository repo = repoCache.get(cacheKey);

		if (repo == null)
		{
			try
			{
				Table t = tables.read(database, table).get();
				repo = factory.newInstance(t);
				repoCache.put(cacheKey, repo);
			}
			catch (InterruptedException | ExecutionException e)
			{
				throw new StorageException(e);
			}
		}

		return repo;
    }
}
