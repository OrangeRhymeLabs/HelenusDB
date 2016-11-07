/*
    Copyright 2016, Strategic Gains, Inc.

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
package com.orangerhymelabs.helenus.cassandra.view;

import java.util.List;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.table.TableService;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.strategicgains.syntaxe.ValidationEngine;
import com.strategicgains.syntaxe.ValidationException;

/**
 * @author tfredrich
 * @since 2 Sept 2016
 */
public class ViewService
{
	private ViewRepository views;
	private TableService tables;

	public ViewService(ViewRepository viewRepository, TableService tableService)
	{
		super();
		this.tables = tableService;
		this.views = viewRepository;
	}

	public void create(View view, FutureCallback<View> callback)
	{
		Futures.addCallback(create(view), callback);
	}

	public ListenableFuture<View> create(View view)
	{
		ListenableFuture<Boolean> tableFuture = tables.exists(view.databaseName(), view.tableName());
		return Futures.transformAsync(tableFuture, new AsyncFunction<Boolean, View>()
		{
			@Override
			public ListenableFuture<View> apply(Boolean exists)
			throws Exception
			{
				if (exists)
				{
					try
					{
						ValidationEngine.validateAndThrow(view);
						return views.create(view);
					}
					catch(ValidationException e)
					{
						return Futures.immediateFailedFuture(e);
					}
				}
				else
				{
					return Futures.immediateFailedFuture(new ItemNotFoundException("Table not found: " + view.tableName()));
				}
			}
		});
	}

	public void read(String database, String table, String view, FutureCallback<View> callback)
	{
		Futures.addCallback(read(database, table, view), callback);
	}

	public ListenableFuture<View> read(String database, String table, String view)
	{
		return views.read(new Identifier(database, table, view));
	}

	public ListenableFuture<List<View>> readAll(String database, String table, Object... parms)
	{
		ListenableFuture<Boolean> tableFuture = tables.exists(database, table);
		return Futures.transformAsync(tableFuture, new AsyncFunction<Boolean, List<View>>()
		{
			@Override
			public ListenableFuture<List<View>> apply(Boolean exists)
			throws Exception
			{
				if (exists)
				{
					return views.readAll(parms);
				}
				else
				{
					return Futures.immediateFailedFuture(new ItemNotFoundException("Table not found: " + table));
				}
			}
		});
	}

	public void readAll(String database, String table, FutureCallback<List<View>> callback)
	{
		Futures.addCallback(readAll(database, table), callback);
	}

	public ListenableFuture<View> update(View view)
	{
		ListenableFuture<Boolean> tableFuture = tables.exists(view.databaseName(), view.tableName());
		return Futures.transformAsync(tableFuture, new AsyncFunction<Boolean, View>()
		{
			@Override
			public ListenableFuture<View> apply(Boolean exists)
			throws Exception
			{
				if (exists)
				{
					try
					{
						ValidationEngine.validateAndThrow(view);
						return views.update(view);
					}
					catch(ValidationException e)
					{
						return Futures.immediateFailedFuture(e);
					}
				}
				else
				{
					return Futures.immediateFailedFuture(new ItemNotFoundException("Database not found: " + view.databaseName()));
				}
			}
		});
	}

	public void update(View view, FutureCallback<View> callback)
	{
		Futures.addCallback(update(view), callback);
	}

	public ListenableFuture<Boolean> delete(String database, String table, String view)
	{
		return views.delete(new Identifier(database, table, view));
	}

	public void delete(String database, String table, String view, FutureCallback<Boolean> callback)
	{
		Futures.addCallback(delete(database, table, view), callback);
	}
}
