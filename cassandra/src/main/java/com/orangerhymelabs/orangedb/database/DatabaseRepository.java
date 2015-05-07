package com.orangerhymelabs.orangedb.database;

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
import com.orangerhymelabs.orangedb.event.EventFactory;
import com.orangerhymelabs.orangedb.persistence.AbstractObservable;
import com.orangerhymelabs.orangedb.persistence.PreparedStatementFactory;

public class DatabaseRepository
extends AbstractObservable<Database>
{
	private Session session;

	private class Tables
	{

		static final String BY_ID = "sys_db";
	}

	private class Schema
	{
		static final String DROP_TABLE = "drop table if exists " + Tables.BY_ID;
		static final String CREATE_TABLE = "create table " + Tables.BY_ID +
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

	private static final String CREATE_CQL = "insert into %s (db_name, description, created_at, updated_at) values (?, ?, ?, ?)";
	private static final String UPDATE_CQL = "update %s set description = ?, updated_at = ? where db_name = ?";
	private static final String READ_CQL = "select * from %s where db_name = ?";
	private static final String READ_ALL_CQL = "select * from %s";
	private static final String DELETE_CQL = "delete from %s where db_name = ?";

	private PreparedStatement createStmt;
	private PreparedStatement updateStmt;
	private PreparedStatement readAllStmt;
	private PreparedStatement readStmt;

	public DatabaseRepository(Session session)
	{
		this.session = session;
		initializeStatements();
	}

	protected void initializeStatements()
	{
		createStmt = session.prepare(String.format(CREATE_CQL, Tables.BY_ID));
		updateStmt = session.prepare(String.format(UPDATE_CQL, Tables.BY_ID));
		readAllStmt = session.prepare(String.format(READ_ALL_CQL, Tables.BY_ID));
		readStmt = session.prepare(String.format(READ_CQL, Tables.BY_ID));
		
	}

	public void initializeTable()
	{
		dropTable();
		createTable();
	}

	public void dropTable()
	{
		session.execute(Schema.DROP_TABLE);
	}

	public void createTable()
	{
		session.execute(Schema.CREATE_TABLE);
	}

	public void create(Database entity, FutureCallback<Database> callback)
	{
		BoundStatement bs = new BoundStatement(createStmt);
		bindCreate(bs, entity);
		ResultSetFuture future = session.executeAsync(bs);
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

	public ResultSetFuture update(Database entity)
	{
		BoundStatement bs = new BoundStatement(updateStmt);
		bindUpdate(bs, entity);
		ResultSetFuture future = session.executeAsync(bs);
		Futures.addCallback(future, callback);
	}

	protected ResultSetFuture delete(String name)
	{
		BoundStatement bs = new BoundStatement(deleteStmt);
		bs.bind(bs, name);
		return session.executeAsync(bs);
	}

	public ResultSetFuture readAll()
	{
		BoundStatement bs = new BoundStatement(readAllStmt);
		return marshalAll(session.execute(bs));
	}

	private void bindCreate(BoundStatement bs, Database entity)
	{
		bs.bind(entity.name(), entity.description(), entity.createdAt(),
		    entity.updatedAt());
	}

	private void bindUpdate(BoundStatement bs, Database entity)
	{
		bs.bind(entity.description(), entity.updatedAt(), entity.name());
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

	protected Database marshalRow(Row row)
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
