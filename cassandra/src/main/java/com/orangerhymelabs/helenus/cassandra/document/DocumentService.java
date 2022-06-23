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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.cassandra.view.ViewService;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.noschema.document.View;
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
	//TODO: Use EhCache (or some other coherent cache implementation)
	private Map<Identifier, AbstractDocumentRepository> repoCache = new HashMap<>();
	private Map<Identifier, List<View>> viewsByTable = new HashMap<>();

	private TableService tables;
	private ViewService views;
	private DocumentRepositoryFactory factory;

	public DocumentService(TableService tableService, ViewService viewService, DocumentRepositoryFactory repositoryFactory)
	{
		super();
		this.tables = tableService;
		this.views = viewService;
		this.factory = repositoryFactory;
	}

	public ListenableFuture<Document> create(String database, String table, Document document)
	{
		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(AbstractDocumentRepository docRepo)
			throws Exception
			{
				try
				{
					ValidationEngine.validateAndThrow(document);
					Document newDoc = docRepo.create(document).get();
					updateViews(newDoc);
					return Futures.immediateFuture(newDoc);
				}
				catch(Exception e)
				{
					return Futures.immediateFailedFuture(e);
				}
			}

			private void updateViews(Document document)
			throws InterruptedException, ExecutionException
			{
				List<View> tableViews = getTableViews(database, table).get();
				tableViews.forEach(new Consumer<View>()
				{
					@Override
					public void accept(View v)
					{
						try
						{
							Identifier id = v.identifierFrom(document);

							if (id != null)
							{
								Document viewDoc = new Document(document.object());
								viewDoc.identifier(id);
								Document newView = createViewDocument(v, viewDoc).get();
								System.out.println(newView.createdAt());
							}
						}
						catch (KeyDefinitionException e)
						{
							e.printStackTrace();
						}
						catch (InterruptedException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						catch (ExecutionException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
			}

//			private ListenableFuture<List<Document>> updateViews(Document document)
//			{
//				ListenableFuture<List<View>> tableViews = getTableViews(database, table);
//				return Futures.transformAsync(tableViews, new AsyncFunction<List<View>, List<Document>>()
//				{
//					@Override
//					public ListenableFuture<List<Document>> apply(List<View> input)
//					throws Exception
//					{
//						List<ListenableFuture<Document>> newDocs = new ArrayList<>(input.size());
//						input.parallelStream().forEach(new Consumer<View>()
//						{
//							@Override
//							public void accept(View v)
//							{
//								try
//								{
//									 Identifier id = v.identifierFrom(document);
//
//									if (id != null)
//									{
//										Document viewDoc = new Document(document.object());
//										viewDoc.identifier(id);
//										newDocs.add(createViewDocument(v, document));
//									}
//								}
//								catch (KeyDefinitionException e)
//								{
//									e.printStackTrace();
//								}
//							}
//						});
//
//						return null;
//					}
//				});
//			}

		}, MoreExecutors.directExecutor());
	}

	private ListenableFuture<Document> createViewDocument(View view, Document document)
	{
		try
		{
			AbstractDocumentRepository docs = acquireRepositoryFor(view).get();
			return docs.create(document);
		}
		catch (InterruptedException e)
		{
			return Futures.immediateFailedFuture(e);
		}
		catch (ExecutionException e)
		{
			return Futures.immediateFailedFuture(e);
		}
//		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(view);
//		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, Document>()
//		{
//			@Override
//			public ListenableFuture<Document> apply(AbstractDocumentRepository docRepo)
//			throws Exception
//			{
//				return docRepo.create(document);
//			}
//		});
	}

	public void create(String database, String table, Document document, FutureCallback<Document> callback)
	{
		Futures.addCallback(create(database, table, document), callback, MoreExecutors.directExecutor());
	}

	public ListenableFuture<Document> read(String database, String table, Identifier id)
	{
		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(AbstractDocumentRepository input)
			throws Exception
			{
				return input.read(new Identifier(id));
			}
		}, MoreExecutors.directExecutor());
	}

	public void read(String database, String table, Identifier id, FutureCallback<Document> callback)
	{
		Futures.addCallback(read(database, table, id), callback, MoreExecutors.directExecutor());
	}

	public ListenableFuture<Document> read(String database, String table, String view, Identifier id)
	{
		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(getTableView(database, table, view));
		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(AbstractDocumentRepository input)
			throws Exception
			{
				return input.read(new Identifier(id));
			}
		}, MoreExecutors.directExecutor());
	}

	public void read(String database, String table, String view, Identifier id, FutureCallback<Document> callback)
	{
		Futures.addCallback(read(database, table, view, id), callback, MoreExecutors.directExecutor());
	}

	public ListenableFuture<List<Document>> readIn(String database, String table, Identifier... ids)
	{
		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, List<Document>>()
		{
			@Override
			public ListenableFuture<List<Document>> apply(AbstractDocumentRepository input)
			throws Exception
			{
				return input.readIn(ids);
			}
		}, MoreExecutors.directExecutor());
	}

	public void readIn(FutureCallback<List<Document>> callback, String database, String table, Identifier... ids)
	{
		Futures.addCallback(readIn(database, table, ids), callback, MoreExecutors.directExecutor());
	}

	public ListenableFuture<Document> update(String database, String table, Document document)
	{
		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(AbstractDocumentRepository input)
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
		}, MoreExecutors.directExecutor());
	}

	public void update(String database, String table, Document document, FutureCallback<Document> callback)
    {
		Futures.addCallback(update(database, table, document), callback, MoreExecutors.directExecutor());
    }

	public ListenableFuture<Document> upsert(String database, String table, Document document)
	{
		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, Document>()
		{
			@Override
			public ListenableFuture<Document> apply(AbstractDocumentRepository input)
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
		}, MoreExecutors.directExecutor());
	}

	public void upsert(String database, String table, Document document, FutureCallback<Document> callback)
    {
		Futures.addCallback(upsert(database, table, document), callback, MoreExecutors.directExecutor());
    }

	public ListenableFuture<Boolean> delete(String database, String table, Identifier id)
	{
		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, Boolean>()
		{
			@Override
			public ListenableFuture<Boolean> apply(AbstractDocumentRepository input)
			throws Exception
			{
				return input.delete(new Identifier(id));
			}
		}, MoreExecutors.directExecutor());
	}

	public void delete(String database, String table, Identifier id, FutureCallback<Boolean> callback)
	{
		Futures.addCallback(delete(database, table, id), callback, MoreExecutors.directExecutor());
	}

	public ListenableFuture<Boolean> exists(String database, String table, Identifier id)
	{
		ListenableFuture<AbstractDocumentRepository> docs = acquireRepositoryFor(database, table);
		return Futures.transformAsync(docs, new AsyncFunction<AbstractDocumentRepository, Boolean>()
		{
			@Override
			public ListenableFuture<Boolean> apply(AbstractDocumentRepository input)
			throws Exception
			{
				return input.exists(new Identifier(id));
			}
		}, MoreExecutors.directExecutor());
	}

	public void exists(String database, String table, Identifier id, FutureCallback<Boolean> callback)
	{
		Futures.addCallback(exists(database, table, id), callback, MoreExecutors.directExecutor());
	}

	private ListenableFuture<AbstractDocumentRepository> acquireRepositoryFor(String database, String table)
    {
		Identifier cacheKey = new Identifier(database, table);
		AbstractDocumentRepository repo = repoCache.get(cacheKey);

		if (repo != null)
		{
			return Futures.immediateFuture(repo);
		}

		ListenableFuture<Table> futureTable = tables.read(database, table);
		return Futures.transformAsync(futureTable, new AsyncFunction<Table, AbstractDocumentRepository>()
		{
			@Override
			public ListenableFuture<AbstractDocumentRepository> apply(Table input)
			throws Exception
			{
				AbstractDocumentRepository repo = factory.newInstance(input);
				repoCache.put(cacheKey, repo);
				return Futures.immediateFuture(repo);
			}
		}, MoreExecutors.directExecutor());
    }

	private ListenableFuture<AbstractDocumentRepository> acquireRepositoryFor(View view)
    {
		Identifier cacheKey = view.identifier();
		AbstractDocumentRepository repo = repoCache.get(cacheKey);

		if (repo != null)
		{
			return Futures.immediateFuture(repo);
		}

		try
		{
			repo = factory.newInstance(view);
			repoCache.put(cacheKey, repo);
			return Futures.immediateFuture(repo);
		}
		catch (KeyDefinitionException e)
		{
			return Futures.immediateFailedFuture(e);
		}
    }

	private ListenableFuture<List<View>> getTableViews(String database, String table)
	{
		Identifier tableId = new Identifier(database, table);
		List<View> cachedViews = viewsByTable.get(tableId);

		if (cachedViews != null)
		{
			return Futures.immediateFuture(cachedViews);
		}

		ListenableFuture<List<View>> allViews = views.readAll(database, table);
		return Futures.transformAsync(allViews, new AsyncFunction<List<View>, List<View>>()
		{
			@Override
			public ListenableFuture<List<View>> apply(List<View> input)
			{
				List<View> cachedViews = new ArrayList<>();
				cachedViews.addAll(input);
				viewsByTable.put(tableId, cachedViews);
				return Futures.immediateFuture(input);
			}
		}, MoreExecutors.directExecutor());
	}

	private View getTableView(String database, String table, String view)
	{
		try {
			for (View v : getTableViews(database, table).get())
			{
				if (v.name().equals(view))
				{
					return v;
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}
}
