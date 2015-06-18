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
package com.orangerhymelabs.orangedb.cassandra.document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.util.concurrent.FutureCallback;
import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.cassandra.table.TableReference;
import com.orangerhymelabs.orangedb.cassandra.table.TableService;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DocumentService
{
	private Map<String, DocumentRepository> repoCache = new HashMap<String, DocumentRepository>();
	private TableService tables;
	private DocumentRepositoryFactory factory;

	public DocumentService(TableService tableService, DocumentRepositoryFactory repositoryFactory)
	{
		super();
		this.tables = tableService;
		this.factory = repositoryFactory;
	}

	public Document create(String database, String table, Document document)
	{
		ValidationEngine.validateAndThrow(document);
		DocumentRepository docs = acquireRepositoryFor(database, table);
		return docs.create(document);
	}

	public void createAsync(String database, String table, Document document, FutureCallback<Document> callback)
	{
		try
		{
			ValidationEngine.validateAndThrow(document);
			DocumentRepository docs = acquireRepositoryFor(database, table);
			docs.createAsync(document, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
	}

	public Document read(String database, String table, Object id)
	{
		DocumentRepository docs = acquireRepositoryFor(database, table);
		return docs.read(new Identifier(database, table, id));
	}

	public void readAsync(String database, String table, Object id, FutureCallback<Document> callback)
	{
		DocumentRepository docs = acquireRepositoryFor(database, table);
		docs.readAsync(new Identifier(database, table, id), callback);
	}

	public List<Document> readIn(String database, String table, Object... ids)
	{
		Identifier[] inIds = new Identifier[ids.length];
		int i = 0;

		for (Object id : ids)
		{
			inIds[i++] = new Identifier(database, table, id);
		}

		DocumentRepository docs = acquireRepositoryFor(database, table);
		return docs.readIn(inIds);
	}

	public void readInAsync(FutureCallback<Document> callback, String database, String table, Object... ids)
	{
		Identifier[] inIds = new Identifier[ids.length];
		int i = 0;

		for (Object id : ids)
		{
			inIds[i++] = new Identifier(database, table, id);
		}

		DocumentRepository docs = acquireRepositoryFor(database, table);
		docs.readInAsync(callback, inIds);
	}

	public void update(String database, String table, Document document)
	{
		ValidationEngine.validateAndThrow(document);
		DocumentRepository docs = acquireRepositoryFor(database, table);
		docs.update(document);
	}

	public void updateAsync(String database, String table, Document document, FutureCallback<Document> callback)
    {
		try
		{
			ValidationEngine.validateAndThrow(document);
			DocumentRepository docs = acquireRepositoryFor(database, table);
			docs.updateAsync(document, callback);
		}
		catch(ValidationException e)
		{
			callback.onFailure(e);
		}
    }

	public void delete(String database, String table, Object id)
	{
		DocumentRepository docs = acquireRepositoryFor(database, table);
		docs.delete(new Identifier(database, table, id));
	}

	public void deleteAsync(String database, String table, Object id, FutureCallback<Document> callback)
	{
		DocumentRepository docs = acquireRepositoryFor(database, table);
		docs.deleteAsync(new Identifier(database, table, id), callback);
	}

	private DocumentRepository acquireRepositoryFor(Table table)
    {
		DocumentRepository repo = repoCache.get(table.toDbTable());

		if (repo == null)
		{
			repo = factory.newDocumentRepositoryFor(table);
			repoCache.put(table.toDbTable(), repo);
		}

		return repo;
    }

	private DocumentRepository acquireRepositoryFor(String database, String table)
    {
		return acquireRepositoryFor(new TableReference(database, table, null).toTable());
    }
}
