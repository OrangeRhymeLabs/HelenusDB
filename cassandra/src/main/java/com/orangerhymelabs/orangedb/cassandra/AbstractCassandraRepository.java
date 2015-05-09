package com.orangerhymelabs.orangedb.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.orangerhymelabs.orangedb.persistence.AbstractObservable;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;

public abstract class AbstractCassandraRepository<T>
extends AbstractObservable<T>
{
	private Session session;
	private String keyspace;

	private PreparedStatement createStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement readAllStmt;
	private PreparedStatement readStmt;
	private PreparedStatement deleteStmt;

	public AbstractCassandraRepository(Session session, String keyspace)
	{
		this.session = session;
		this.keyspace = keyspace;
		initializeStatements();
	}

	protected void initializeStatements()
	{
		createStmt = prepare(buildCreateStatement());
		updateStmt = prepare(buildUpdateStatement());
		readAllStmt = prepare(buildReadAllStatement());
		readStmt = prepare(buildReadStatement());
		deleteStmt = prepare(buildDeleteStatement());
	}

//	public void exists(Identifier id, )
//	{
//		if (id == null || id.isEmpty()) return false;
//
//		BoundStatement bs = new BoundStatement(existStmt);
//		bindIdentity(bs, id);
//		return (getSession().execute(bs).one().getLong(0) > 0);
//	}

	public void create(T entity, ResultCallback<T> callback)
	{
		ResultSetFuture future = create(entity);
		handleFuture(future, callback);
	}

	public ResultSetFuture create(T entity)
	{
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		return session.executeAsync(bs);
	}

	public void update(T entity, ResultCallback<T> callback)
	{
		ResultSetFuture future = update(entity);
		handleFuture(future, callback);
	}

	public ResultSetFuture update(T entity)
	{
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		return session.executeAsync(bs);
	}

	public void delete(Identifier id, ResultCallback<T> callback)
	{
		ResultSetFuture future = delete(id);
		handleFuture(future, callback);
	}

	public ResultSetFuture delete(Identifier id)
	{
		BoundStatement bs = new BoundStatement(deleteStmt);
		bindIdentity(bs, id);
		return session.executeAsync(bs);
	}

	public void read(Identifier id, ResultCallback<T> callback)
	{
		ResultSetFuture future = read(id);
		handleFuture(future, callback);
	}

	public ResultSetFuture read(Identifier id)
	{
		BoundStatement bs = new BoundStatement(readStmt);
		bindIdentity(bs, id);
		return session.executeAsync(bs);
	}

	public void readAll(ResultCallback<List<T>> callback)
	{
		ResultSetFuture future = readAll();
		Futures.addCallback(future,
			new FutureCallback<ResultSet>()
			{
				@Override
				public void onSuccess(ResultSet result)
				{
					callback.onSuccess(marshalAll(result));
				}
	
				@Override
				public void onFailure(Throwable t)
				{
					callback.onFailure(t);
				}
			},
			MoreExecutors.sameThreadExecutor()
		);
	}

	public ResultSetFuture readAll()
	{
		BoundStatement bs = new BoundStatement(readAllStmt);
		return session.executeAsync(bs);
	}

	public Session session()
	{
		return session;
	}

	protected String keyspace()
	{
		return keyspace;
	}

	protected void handleFuture(ResultSetFuture future, ResultCallback<T> callback)
    {
	    Futures.addCallback(future,
			new FutureCallback<ResultSet>()
			{
				@Override
				public void onSuccess(ResultSet result)
				{
					callback.onSuccess(marshalRow(result.one()));
				}
	
				@Override
				public void onFailure(Throwable t)
				{
					callback.onFailure(t);
				}
			},
			MoreExecutors.sameThreadExecutor()
		);
    }

	protected void bindIdentity(BoundStatement bs, Identifier id)
	{
		bs.bind(id.components().toArray());
	}

	protected abstract void bindCreate(BoundStatement bs, T entity);

	protected abstract void bindUpdate(BoundStatement bs, T entity);

	protected List<T> marshalAll(ResultSet rs)
	{
		List<T> dbs = new ArrayList<T>();
		Iterator<Row> i = rs.iterator();

		while (i.hasNext())
		{
			dbs.add(marshalRow(i.next()));
		}

		return dbs;
	}

	protected abstract T marshalRow(Row row);
	protected abstract String buildCreateStatement();
	protected abstract String buildUpdateStatement();
	protected abstract String buildReadStatement();
	protected abstract String buildReadAllStatement();
	protected abstract String buildDeleteStatement();

	private PreparedStatement prepare(String statement)
	{
		return session().prepare(statement);		
	}
}
