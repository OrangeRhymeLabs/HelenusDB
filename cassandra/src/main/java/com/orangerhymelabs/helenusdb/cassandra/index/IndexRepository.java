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
package com.orangerhymelabs.helenusdb.cassandra.index;

import java.util.Date;
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
import com.orangerhymelabs.helenusdb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenusdb.cassandra.DataTypes;
import com.orangerhymelabs.helenusdb.cassandra.Schemaable;
import com.orangerhymelabs.helenusdb.exception.StorageException;
import com.orangerhymelabs.helenusdb.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class IndexRepository
extends AbstractCassandraRepository<Index>
{
	private class Tables
	{
		static final String BY_ID = "sys_idx";
	}

	public static class Schema
	implements Schemaable
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table if not exists %s." + Tables.BY_ID +
			"(" +
				"db_name text," +
				"tbl_name text," +
				"name text," +
				"description text," +
				"is_unique boolean," +
				"fields list<text>," +
				"id_type text," +
				"engine text," +
				"created_at timestamp," +
				"updated_at timestamp," +
				"primary key ((db_name), tbl_name, name)" +
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

	private class Columns
	{
		static final String DB_NAME = "db_name";
		static final String TBL_NAME = "tbl_name";
		static final String NAME = "name";
		static final String DESCRIPTION = "description";
		static final String IS_UNIQUE = "is_unique";
		static final String FIELDS = "fields";
		static final String ID_TYPE = "id_type";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String IDENTITY_CQL = " where " + Columns.DB_NAME + "= ? and " + Columns.TBL_NAME + " = ? and " + Columns.NAME + " = ?";
	private static final String CREATE_CQL = "insert into %s.%s (" + Columns.DB_NAME + ", " + Columns.TBL_NAME + ", " + Columns.NAME + ", " + Columns.DESCRIPTION + ", " + Columns.FIELDS + ", " + Columns.ID_TYPE + ", " + Columns.IS_UNIQUE + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (?, ?, ?, ?, ?, ?, ?, ?, ?) if not exists";
	private static final String UPDATE_CQL = "update %s.%s set " + Columns.DESCRIPTION + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL;
	private static final String READ_FOR_TABLE_CQL = "select * from %s.%s where " + Columns.DB_NAME + "= ? and " + Columns.TBL_NAME + " = ?";
	private static final String READ_ALL_CQL = "select * from %s.%s";
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;

	private PreparedStatement readForTableStmt;

	public IndexRepository(Session session, String keyspace)
	{
		super(session, keyspace);
		readForTableStmt = prepare(String.format(READ_FOR_TABLE_CQL, keyspace(), Tables.BY_ID));
	}

	@Override
	public void createAsync(Index index, FutureCallback<Index> callback)
	{
		super.createAsync(index, callback);
	}

	@Override
	public Index create(Index index)
	{
		return super.create(index);
	}

	/**
	 * Read all the indexes for a given database table, returning the results as a list.
	 * 
	 * @param database the database name.
	 * @param table the table name.
	 * @return A list of Index instance. Possibly empty. Never null.
	 */
	public List<Index> readFor(String database, String table)
	{
		try
		{
			ResultSet rs = _readFor(database, table).get();
			return marshalAll(rs);
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new StorageException(e);
		}
	}

	/**
	 * Read all the indexes for a given database table, calling the callback with the results as a list.
	 * 
	 * @param database the database name.
	 * @param table the table name.
	 * @param callback the callback to notify when results are available.
	 */
	public void readForAsync(String database, String table, FutureCallback<List<Index>> callback)
	{
		ResultSetFuture future = _readFor(database, table);
		Futures.addCallback(future, new FutureCallback<ResultSet>()
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
		}, MoreExecutors.sameThreadExecutor());
	}

	private ResultSetFuture _readFor(String database, String table)
	{
		BoundStatement bs = new BoundStatement(readForTableStmt);
		bs.bind(database, table);
		return session().executeAsync(bs);
	}

	@Override
	public void delete(Identifier id)
	{
		super.delete(id);
	}

	@Override
	public void deleteAsync(Identifier id, FutureCallback<Index> callback)
	{
		super.deleteAsync(id, callback);
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
	protected void bindCreate(BoundStatement bs, Index index)
	{
		Date now = new Date();
		index.createdAt(now);
		index.updatedAt(now);
		bs.bind(index.databaseName(),
			index.tableName(),
			index.name(),
			index.description(),
			index.fields(),
			index.idType().name(),
			index.isUnique(),
			index.createdAt(),
		    index.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Index index)
	{
		index.updatedAt(new Date());
		bs.bind(index.description(),
		    index.updatedAt(),
		    index.databaseName(),
		    index.tableName(),
		    index.name());
	}

	protected Index marshalRow(Row row)
	{
		if (row == null) return null;

		Index n = new Index();
		n.table(row.getString(Columns.DB_NAME), row.getString(Columns.TBL_NAME), DataTypes.from(row.getString(Columns.ID_TYPE)));
		n.name(row.getString(Columns.NAME));
		n.description(row.getString(Columns.DESCRIPTION));
		n.fields(row.getList(Columns.FIELDS, String.class));
		n.isUnique(row.getBool(Columns.IS_UNIQUE));
		n.createdAt(row.getDate(Columns.CREATED_AT));
		n.updatedAt(row.getDate(Columns.UPDATED_AT));
		return n;
	}
}
