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
package com.orangerhymelabs.helenus.cassandra;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Function;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.InvalidIdentifierException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.orangerhymelabs.helenus.persistence.StatementFactory;
import com.orangerhymelabs.helenus.persistence.StatementFactoryHandler;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 * @param <T> The type stored in this repository.
 */
public abstract class AbstractCassandraRepository<T, F extends StatementFactory>
{
	private Session session;
	private String keyspace;
	private F statementFactory;

	public AbstractCassandraRepository(Session session, String keyspace, Class<F> factoryClass)
	{
		this(session, keyspace, null, factoryClass);
	}

	public AbstractCassandraRepository(Session session, String keyspace, String table, Class<F> factoryClass)
	{
		this.session = session;
		this.keyspace = keyspace;
		this.statementFactory = newStatementFactory(factoryClass, session, keyspace, table);
	}

	public AbstractCassandraRepository(Session session, String keyspace, F factory)
	{
		this.session = session;
		this.keyspace = keyspace;
		this.statementFactory = factory;
	}

	protected AbstractCassandraRepository(Session session, String keyspace)
	{
		this(session, keyspace, (F) null);
	}

	public ListenableFuture<T> create(T entity)
	{
		ListenableFuture<ResultSet> future = submitCreate(entity);
		return Futures.transformAsync(future, new AsyncFunction<ResultSet, T>()
		{
			@Override
			public ListenableFuture<T> apply(ResultSet result)
			throws Exception
			{
				if (result.wasApplied())
				{
					return Futures.immediateFuture(entity);
				}

				return Futures.immediateFailedFuture(new DuplicateItemException(entity.toString()));
			}
		}, MoreExecutors.directExecutor());
	}

	public ListenableFuture<Boolean> exists(Identifier id)
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

	public ListenableFuture<T> update(T entity)
	{
		ListenableFuture<ResultSet> future = submitUpdate(entity);
		return Futures.transformAsync(future, new AsyncFunction<ResultSet, T>()
		{
			@Override
			public ListenableFuture<T> apply(ResultSet result)
			{
				if (result.wasApplied())
				{
					return Futures.immediateFuture(entity);
				}

				return Futures.immediateFailedFuture(new ItemNotFoundException(entity.toString()));
			}
		}, MoreExecutors.directExecutor());		
	}

	public ListenableFuture<Boolean> delete(Identifier id)
	{
		ListenableFuture<ResultSet> future = submitDelete(id);
		return Futures.transformAsync(future, new AsyncFunction<ResultSet, Boolean>()
		{
			@Override
			public ListenableFuture<Boolean> apply(ResultSet result)
			{
				if (!result.wasApplied())
				{
					return Futures.immediateFailedFuture(new ItemNotFoundException(id.toString()));
				}

				return Futures.immediateFuture(true);
			}
		}, MoreExecutors.directExecutor());
	}

	public ListenableFuture<T> read(Identifier id)
	{
		ListenableFuture<ResultSet> rs = submitRead(id);
		return Futures.transformAsync(rs, new AsyncFunction<ResultSet, T>()
		{
			@Override
			public ListenableFuture<T> apply(ResultSet result)
			{
				if (result.isExhausted())
				{
					return Futures.immediateFailedFuture(new ItemNotFoundException(id.toString()));
				}

				return Futures.immediateFuture(marshalRow(result.one()));
			}
		}, MoreExecutors.directExecutor());
	}

	public ListenableFuture<List<T>> readAll(Object... parms)
	{
		return readAll(statementFactory.readAll(), parms);
	}

	public ListenableFuture<List<T>> readAll(PreparedStatement statement, Object... parms)
	{
		ListenableFuture<ResultSet> future = submitStatement(statement, parms);
		return Futures.transformAsync(future, new AsyncFunction<ResultSet, List<T>>()
		{
			@Override
			public ListenableFuture<List<T>> apply(ResultSet input)
			{
				return Futures.immediateFuture(marshalAll(input));
			}
		}, MoreExecutors.directExecutor());
	}

	/**
	 * Read all given identifiers.
	 * 
	 * Leverages the token-awareness of the driver to optimally query each node directly instead of invoking a
	 * coordinator node. Sends an individual query for each partition key, so reaches the appropriate replica
	 * directly and collates the results client-side.
	 * 
	 * @param ids the partition keys (identifiers) to select.
	 */
	public ListenableFuture<List<T>> readIn(Identifier... ids)
	{
		List<ListenableFuture<ResultSet>> futures = submitReadIn(ids);
		List<ListenableFuture<T>> results = new ArrayList<>(ids.length);

		for (ListenableFuture<ResultSet> future : futures)
		{
			results.add(Futures.transformAsync(future, new AsyncFunction<ResultSet, T>()
			{
				@Override
				public ListenableFuture<T> apply(ResultSet input)
				{
					if (!input.isExhausted())
					{
						return Futures.immediateFuture(marshalRow(input.one()));
					}

					return Futures.immediateFuture(null);
				}
			}, MoreExecutors.directExecutor()));
		}

		return Futures.allAsList(results);
	}

	public Session session()
	{
		return session;
	}

	protected String keyspace()
	{
		return keyspace;
	}

	protected F statementFactory()
	{
		return statementFactory;
	}

	protected void statementFactory(F factory)
	{
		this.statementFactory = factory;
	}

	protected void bindIdentity(BoundStatement bs, Identifier id)
	{
		try
		{
			bs.bind(id.components().toArray());
		}
		catch(InvalidTypeException | CodecNotFoundException e)
		{
			throw new InvalidIdentifierException(e);
		}
	}

	protected List<T> marshalAll(ResultSet rs)
	{
		List<T> results = new ArrayList<T>();
		Iterator<Row> i = rs.iterator();

		while (i.hasNext())
		{
			results.add(marshalRow(i.next()));
		}

		return results;
	}

	protected abstract void bindCreate(BoundStatement bs, T entity);
	protected abstract void bindUpdate(BoundStatement bs, T entity);
	protected abstract T marshalRow(Row row);

	protected ResultSetFuture submitCreate(T entity)
	{
		BoundStatement bs = new BoundStatement(statementFactory.create());
		bindCreate(bs, entity);
		return session.executeAsync(bs);
	}

	protected ResultSetFuture submitDelete(Identifier id)
	{
		BoundStatement bs = new BoundStatement(statementFactory.delete());
		bindIdentity(bs, id);
		return session.executeAsync(bs);
	}

	private ListenableFuture<ResultSet> submitExists(Identifier id)
	{
		BoundStatement bs = new BoundStatement(statementFactory().exists());
		bindIdentity(bs, id);
		return session().executeAsync(bs);
	}

	private ResultSetFuture submitRead(Identifier id)
	{
		BoundStatement bs = new BoundStatement(statementFactory.read());
		bindIdentity(bs, id);
		return session.executeAsync(bs);
	}

	protected ResultSetFuture submitStatement(PreparedStatement statement, Object... parms)
	{
		BoundStatement bs = new BoundStatement(statement);

		if (parms != null)
		{
			bs.bind(parms);
		}

		return session.executeAsync(bs);
	}

	protected ResultSetFuture submitUpdate(T entity)
	{
		BoundStatement bs = new BoundStatement(statementFactory.update());
		bindUpdate(bs, entity);
		return session.executeAsync(bs);
	}

	/**
	 * Leverages the token-awareness of the driver to optimally query each node directly instead of invoking a
	 * coordinator node. Sends an individual query for each partition key, so reaches the appropriate replica
	 * directly and collates the results client-side.
	 * 
	 * @param ids the partition keys (identifiers) to select.
	 * @return a List of ListenableFuture instances for each underlying ResultSet--one for each ID.
	 */
	private  List<ListenableFuture<ResultSet>> submitReadIn(Identifier... ids)
	{
		if (ids == null) return Collections.emptyList();

		List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>(ids.length);

		for (Identifier id : ids)
		{
			BoundStatement bs = new BoundStatement(statementFactory.read());
			bindIdentity(bs, id);
			futures.add(session.executeAsync(bs));
		}

		return Futures.inCompletionOrder(futures);
	}

	@SuppressWarnings("unchecked")
	private F newStatementFactory(Class<F> factoryClass, Session session, String keyspace, String table)
	{
		return (F) Proxy.newProxyInstance(factoryClass.getClassLoader(), (Class<F>[]) new Class[]{factoryClass}, new StatementFactoryHandler(session, keyspace, table));
	}
}
