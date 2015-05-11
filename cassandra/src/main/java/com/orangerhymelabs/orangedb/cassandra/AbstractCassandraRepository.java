package com.orangerhymelabs.orangedb.cassandra;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.orangerhymelabs.orangedb.exception.DuplicateItemException;
import com.orangerhymelabs.orangedb.exception.ItemNotFoundException;
import com.orangerhymelabs.orangedb.exception.StorageException;
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

	public void createAsync(T entity, ResultCallback<T> callback)
	{
		ResultSetFuture future = _create(entity);
	    Futures.addCallback(future,
			new FutureCallback<ResultSet>()
			{
				@Override
				public void onSuccess(ResultSet result)
				{
					if (!result.wasApplied())
					{
						callback.onFailure(new DuplicateItemException(entity.toString()));
					}

					callback.onSuccess(null);
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

	public T create(T entity)
	{
		try
        {
	        _create(entity).get();
	        return entity;
        }
        catch (InterruptedException | ExecutionException e)
        {
        	throw new StorageException(e);
        }		
	}

	private ResultSetFuture _create(T entity)
	{
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		return session.executeAsync(bs);
	}

	public void updateAsync(T entity, ResultCallback<T> callback)
	{
		ResultSetFuture future = _update(entity);
	    Futures.addCallback(future,
			new FutureCallback<ResultSet>()
			{
				@Override
				public void onSuccess(ResultSet result)
				{
					if (!result.wasApplied())
					{
						callback.onFailure(new ItemNotFoundException(entity.toString()));
					}

					callback.onSuccess(null);
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

	public T update(T entity)
	{
		try
        {
	        _update(entity).get();
	        return entity;
        }
        catch (InterruptedException | ExecutionException e)
        {
        	throw new StorageException(e);
        }		
	}

	private ResultSetFuture _update(T entity)
	{
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		return session.executeAsync(bs);
	}

	public void deleteAsync(Identifier id, ResultCallback<T> callback)
	{
		ResultSetFuture future = _delete(id);
	    Futures.addCallback(future,
			new FutureCallback<ResultSet>()
			{
				@Override
				public void onSuccess(ResultSet result)
				{
					if (!result.wasApplied())
					{
						callback.onFailure(new ItemNotFoundException(id.toString()));
					}

					callback.onSuccess(null);
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

	public void delete(Identifier id)
	{
		try
        {
	        _delete(id).get();
        }
        catch (InterruptedException | ExecutionException e)
        {
        	throw new StorageException(e);
        }
	}

	private ResultSetFuture _delete(Identifier id)
	{
		BoundStatement bs = new BoundStatement(deleteStmt);
		bindIdentity(bs, id);
		return session.executeAsync(bs);
	}

	public void readAsync(Identifier id, ResultCallback<T> callback)
	{
		ResultSetFuture future = _read(id);
	    Futures.addCallback(future,
			new FutureCallback<ResultSet>()
			{
				@Override
				public void onSuccess(ResultSet result)
				{
					if (result.isExhausted())
					{
						callback.onFailure(new ItemNotFoundException(id.toString()));
					}

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

	public T read(Identifier id)
	{
		try
		{
			ResultSet rs = _read(id).get();

			if (rs.isExhausted())
			{
				throw new ItemNotFoundException(id.toString());
			}

			return marshalRow(rs.one());
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new StorageException(e);
		}
	}

	private ResultSetFuture _read(Identifier id)
	{
		BoundStatement bs = new BoundStatement(readStmt);
		bindIdentity(bs, id);
		return session.executeAsync(bs);
	}

	public void readAllAsync(ResultCallback<List<T>> callback)
	{
		ResultSetFuture future = _readAll();
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

	public List<T> readAll()
	{
		try
        {
	        ResultSet rs = _readAll().get();
	        return marshalAll(rs);
        }
        catch (InterruptedException | ExecutionException e)
        {
        	throw new StorageException(e);
        }
	}

	private ResultSetFuture _readAll()
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

	protected void handleReadFuture(ResultSetFuture future, ResultCallback<T> callback)
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

	protected void handleWriteFuture(ResultSetFuture future, ResultCallback<T> callback)
    {
	    Futures.addCallback(future,
			new FutureCallback<ResultSet>()
			{
				@Override
				public void onSuccess(ResultSet result)
				{
					if (!result.wasApplied())
					{
						
					}
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
