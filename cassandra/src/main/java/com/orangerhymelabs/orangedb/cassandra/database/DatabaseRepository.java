package com.orangerhymelabs.orangedb.cassandra.database;

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
import com.orangerhymelabs.orangedb.cassandra.event.EventFactory;
import com.orangerhymelabs.orangedb.cassandra.event.StateChangeEventingObserver;
import com.orangerhymelabs.orangedb.cassandra.persistence.Schemaable;
import com.orangerhymelabs.orangedb.persistence.AbstractObservable;
import com.orangerhymelabs.orangedb.persistence.Identifier;
import com.orangerhymelabs.orangedb.persistence.ResultCallback;

public class DatabaseRepository
extends AbstractObservable<Database>
implements Schemaable
{
	private Session session;

	private class Tables
	{

		static final String BY_ID = "sys_db";
	}

	private class Schema
	{
		static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		static final String CREATE_TABLE = "create table %s." + Tables.BY_ID +
			"(" +
				"db_name text," +
				"description text," +
				"created_at timestamp," +
				"updated_at timestamp," +
				"primary key ((db_name), updated_at)" +
			")" +
			"with clustering order by (updated_at DESC)";
	}

	private class Columns
	{
		static final String NAME = "db_name";
		static final String DESCRIPTION = "description";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where db_name = ?";
	private static final String CREATE_CQL = "insert into %s.%s (db_name, description, created_at, updated_at) values (?, ?, ?, ?)";
	private static final String UPDATE_CQL = "update %s.%s set description = ?, updated_at = ?" + IDENTITY_CQL;
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL + " limit 1";
	private static final String READ_ALL_CQL = "select * from %s.%s";
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;

	private PreparedStatement createStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement readAllStmt;
	private PreparedStatement readStmt;
	private PreparedStatement deleteStmt;
	private String keyspace;

	public DatabaseRepository(Session session, String keyspace)
	{
		this.session = session;
		this.keyspace = keyspace;
		addObserver(new StateChangeEventingObserver<Database>(new DatabaseEventFactory()));
		initializeStatements();
	}

	protected void initializeStatements()
	{
		createStmt = session.prepare(String.format(CREATE_CQL, keyspace, Tables.BY_ID));
		updateStmt = session.prepare(String.format(UPDATE_CQL, keyspace, Tables.BY_ID));
		readAllStmt = session.prepare(String.format(READ_ALL_CQL, keyspace, Tables.BY_ID));
		readStmt = session.prepare(String.format(READ_CQL, keyspace, Tables.BY_ID));
		deleteStmt = session.prepare(String.format(DELETE_CQL, keyspace, Tables.BY_ID));		
	}

	@Override
	public boolean dropSchema()
	{
		ResultSet rs = session.execute(String.format(Schema.DROP_TABLE, keyspace));
		return rs.wasApplied();
	}

	@Override
	public boolean createSchema()
	{
		ResultSet rs = session.execute(String.format(Schema.CREATE_TABLE, keyspace));
		return rs.wasApplied();
	}

	public void create(Database db, ResultCallback<Database> callback)
	{
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, db);
		ResultSetFuture future = session.executeAsync(bs);
		handleFuture(future, callback);
	}

	public void update(Database entity, ResultCallback<Database> callback)
	{
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		ResultSetFuture future = session.executeAsync(bs);
		handleFuture(future, callback);
	}

	public void delete(Identifier id, ResultCallback<Database> callback)
	{
		BoundStatement bs = new BoundStatement(deleteStmt);
		bindIdentity(bs, id);
		ResultSetFuture future = session.executeAsync(bs);
		handleFuture(future, callback);
	}

	public void read(Identifier id, ResultCallback<Database> callback)
	{
		BoundStatement bs = new BoundStatement(readStmt);
		bindIdentity(bs, id);
		ResultSetFuture future = session.executeAsync(bs);
		handleFuture(future, callback);
	}

	public void readAll(ResultCallback<List<Database>> callback)
	{
		BoundStatement bs = new BoundStatement(readAllStmt);
		ResultSetFuture future = session.executeAsync(bs);
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

	private void handleFuture(ResultSetFuture future, ResultCallback<Database> callback)
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

	private void bindIdentity(BoundStatement bs, Identifier id)
	{
		bs.bind(id.components().toArray());
	}

	private void bindCreate(BoundStatement bs, Database entity)
	{
		bs.bind(entity.name(),
			entity.description(),
			entity.createdAt(),
		    entity.updatedAt());
	}

	private void bindUpdate(BoundStatement bs, Database entity)
	{
		bs.bind(entity.description(),
			entity.updatedAt(),
			entity.name());
	}

	private List<Database> marshalAll(ResultSet rs)
	{
		List<Database> dbs = new ArrayList<Database>();
		Iterator<Row> i = rs.iterator();

		while (i.hasNext())
		{
			dbs.add(marshalRow(i.next()));
		}

		return dbs;
	}

	private Database marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		Database n = new Database();
		n.name(row.getString(Columns.NAME));
		n.description(row.getString(Columns.DESCRIPTION));
		n.createdAt(row.getDate(Columns.CREATED_AT));
		n.updatedAt(row.getDate(Columns.UPDATED_AT));
		return n;
	}

	private class DatabaseEventFactory
	implements EventFactory<Database>
	{

		@Override
		public Object newCreatedEvent(Database object)
		{
			return new DatabaseCreatedEvent(object);
		}

		@Override
		public Object newUpdatedEvent(Database object)
		{
			return new DatabaseUpdatedEvent(object);
		}

		@Override
		public Object newDeletedEvent(Database object)
		{
			return new DatabaseDeletedEvent(object);
		}
	}
}
