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
package com.orangerhymelabs.helenusdb.cassandra.database;

import java.util.Date;
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
import com.orangerhymelabs.helenusdb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenusdb.cassandra.SchemaProvider;
import com.orangerhymelabs.helenusdb.exception.StorageException;
import com.orangerhymelabs.helenusdb.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DatabaseRepository
extends AbstractCassandraRepository<Database>
{
	private class Tables
	{

		static final String BY_ID = "sys_db";
	}

	private class Columns
	{
		static final String NAME = "db_name";
		static final String DESCRIPTION = "description";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	public static class Schema
	implements SchemaProvider
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table if not exists %s." + Tables.BY_ID +
			"(" +
				Columns.NAME + " text," +
				Columns.DESCRIPTION + " text," +
				Columns.CREATED_AT + " timestamp," +
				Columns.UPDATED_AT + " timestamp," +
				"primary key (" + Columns.NAME + ")" +
			")";

		@Override
		public boolean drop(Session session, String keyspace)
		{
			ResultSet rs = session.execute(String.format(Schema.DROP_TABLE, keyspace));
			return rs.wasApplied();
		}

		@Override
		public boolean create(Session session, String keyspace)
		{
			ResultSet rs = session.execute(String.format(Schema.CREATE_TABLE, keyspace));
			return rs.wasApplied();
		}
	}

	private static final String IDENTITY_CQL = " where " + Columns.NAME + " = ?";
	private static final String CREATE_CQL = "insert into %s.%s (" + Columns.NAME + ", " + Columns.DESCRIPTION + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?) if not exists";
	private static final String UPDATE_CQL = "update %s.%s set " + Columns.DESCRIPTION + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL;
	private static final String READ_ALL_CQL = "select * from %s.%s";
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;
	private static final String EXISTS_CQL = "select count(*) from %s.%s" + IDENTITY_CQL + " limit 1";

	private PreparedStatement existsStmt;

	public DatabaseRepository(Session session, String keyspace)
	{
		super(session, keyspace);
		this.existsStmt = prepare(String.format(EXISTS_CQL, keyspace(), Tables.BY_ID));
	}

	public boolean exists(Identifier id)
	{
		ResultSet rs;
        try
        {
	        rs = _exists(id).get();
	        return (rs.one().getLong(0) > 0);
        }
        catch (InterruptedException | ExecutionException e)
        {
        	throw new StorageException(e);
        }
	}

	public void existsAsync(Identifier id, FutureCallback<Boolean> callback)
    {
		ResultSetFuture future = _exists(id);
		Futures.addCallback(future, new FutureCallback<ResultSet>()
		{
			@Override
			public void onSuccess(ResultSet result)
			{
				callback.onSuccess(result.one().getLong(0) > 0);
			}

			@Override
			public void onFailure(Throwable t)
			{
				callback.onFailure(t);
			}
		}, MoreExecutors.sameThreadExecutor());
    }

	private ResultSetFuture _exists(Identifier id)
	{
		BoundStatement bs = new BoundStatement(existsStmt);
		bindIdentity(bs, id);
		return session().executeAsync(bs);
	}

	@Override
	protected String buildCreateStatement()
	{
		return String.format(CREATE_CQL, keyspace(), Tables.BY_ID);
	}

	@Override
	protected String buildUpdateStatement()
	{
		return String.format(UPDATE_CQL, keyspace(), Tables.BY_ID);
	}

	@Override
	protected String buildReadStatement()
	{
		return String.format(READ_CQL, keyspace(), Tables.BY_ID);
	}

	protected String buildReadAllStatement()
	{
		return String.format(READ_ALL_CQL, keyspace(), Tables.BY_ID);
	}

	protected String buildDeleteStatement()
	{
		return String.format(DELETE_CQL, keyspace(), Tables.BY_ID);
	}

	@Override
	protected void bindCreate(BoundStatement bs, Database entity)
	{
		Date now = new Date();
		entity.createdAt(now);
		entity.updatedAt(now);
		bs.bind(entity.name(),
			entity.description(),
			entity.createdAt(),
		    entity.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Database entity)
	{
		entity.updatedAt(new Date());
		bs.bind(entity.description(),
		    entity.updatedAt(),
		    entity.name());
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
}
