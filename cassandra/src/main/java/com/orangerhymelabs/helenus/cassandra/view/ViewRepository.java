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
package com.orangerhymelabs.helenus.cassandra.view;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.SchemaProvider;
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.view.ViewRepository.ViewStatements;
import com.orangerhymelabs.helenus.cassandra.view.key.KeyDefinitionException;
import com.orangerhymelabs.helenus.cassandra.view.key.KeyDefinitionParser;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.orangerhymelabs.helenus.persistence.Query;
import com.orangerhymelabs.helenus.persistence.StatementFactory;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class ViewRepository
extends AbstractCassandraRepository<View, ViewStatements>
{
	private static final Logger LOG = LoggerFactory.getLogger(ViewRepository.class);
	private static final KeyDefinitionParser KEY_PARSER = new KeyDefinitionParser();

	private class Tables
	{
		static final String BY_ID = "sys_view";
	}

	private class Columns
	{
		static final String DATABASE = "db_name";
		static final String TABLE = "tbl_name";
		static final String NAME = "view_name";
		static final String DESCRIPTION = "description";
		static final String KEYS = "keys";
		static final String TTL = "view_ttl";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	public static class Schema
	implements SchemaProvider
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table %s." + Tables.BY_ID +
			"(" +
				Columns.DATABASE + " text," +
				Columns.TABLE + " text," +
				Columns.NAME + " text," +
				Columns.DESCRIPTION + " text," +
				Columns.KEYS + " text," +
				Columns.TTL + " bigint," +
				Columns.CREATED_AT + " timestamp," +
				Columns.UPDATED_AT + " timestamp," +
				"primary key ((" + Columns.DATABASE + "), " + Columns.TABLE + "," + Columns.NAME + ")" +
			")";

		@Override
	    public boolean drop(Session session, String keyspace)
	    {
			ResultSetFuture rs = session.executeAsync(String.format(DROP_TABLE, keyspace));
		    try
		    {
				return rs.get().wasApplied();
			}
		    catch (InterruptedException | ExecutionException e)
		    {
		    	LOG.error("Table schema drop failed", e);
			}

		    return false;
	    }

		@Override
	    public boolean create(Session session, String keyspace)
	    {
			ResultSetFuture rs = session.executeAsync(String.format(CREATE_TABLE, keyspace));
		    try
		    {
				return rs.get().wasApplied();
			}
		    catch (InterruptedException | ExecutionException e)
		    {
		    	LOG.error("Table schema create failed", e);
			}

		    return false;
	    }
	}

	private static final String IDENTITY_CQL = " where " + Columns.DATABASE + " = ? and " + Columns.TABLE + " = ? and " + Columns.NAME + " = ?";

	public interface ViewStatements
	extends StatementFactory
	{
		@Override
		@Query("insert into %s." + Tables.BY_ID + " ("
		+ Columns.DATABASE + ", "
		+ Columns.TABLE + ", "
		+ Columns.NAME + ", "
		+ Columns.DESCRIPTION + ", "
		+ Columns.KEYS + ", "
		+ Columns.TTL + ", "
		+ Columns.CREATED_AT + ", "
		+ Columns.UPDATED_AT
		+") values (?, ?, ?, ?, ?, ?, ?, ?) if not exists")
		PreparedStatement create();

		@Override
		@Query("delete from %s." + Tables.BY_ID + IDENTITY_CQL)
		PreparedStatement delete();

		@Override
		@Query("update %s." + Tables.BY_ID + " set " + Columns.DESCRIPTION + " = ?, " + Columns.TTL + " = ?, " + Columns.UPDATED_AT + " = ?" + IDENTITY_CQL + " if exists")
		PreparedStatement update();

		@Override
		@Query("select * from %s." + Tables.BY_ID + IDENTITY_CQL)
		PreparedStatement read();

		@Override
		@Query("select * from %s." + Tables.BY_ID + " where " + Columns.DATABASE + " = ?")
		PreparedStatement readAll();

		@Query("select * from %s." + Tables.BY_ID + " where " + Columns.DATABASE + " = ?" + Columns.TABLE + " = ?")
		PreparedStatement readAllForTable();
	}

	private static final ViewDocumentRepository.Schema DOCUMENT_SCHEMA = new ViewDocumentRepository.Schema();

	public ViewRepository(Session session, String keyspace)
	{
		super(session, keyspace, ViewStatements.class);
	}

	@Override
	public ListenableFuture<View> create(View view)
	{
		if (createDocumentSchema(view))
		{
			// TODO: what about rollback?
			return super.create(view);
		}
		else
		{
			return Futures.immediateFailedFuture(new StorageException("Failed to create document schema for view: " + view.name()));
		}
	}

	@Override
	public ListenableFuture<Boolean> delete(Identifier id)
	{
		if (dropDocumentSchema(id))
		{
			// TODO: what about rollback?
			return super.delete(id);
		}
		else
		{
			return Futures.immediateFailedFuture(new StorageException("Failed to drop document schema for: " + id.toDbName()));
		}
	}

	public ListenableFuture<List<View>> readForTable(String database, String table)
	{
		return super.readAll(statementFactory().readAllForTable(), database, table);
	}

	@Override
	protected void bindCreate(BoundStatement bs, View view)
	{
		Date now = new Date();
		view.createdAt(now);
		view.updatedAt(now);
		bs.bind(view.databaseName(),
			view.tableName(),
			view.name(),
			view.description(),
			view.keys(),
			view.ttl(),
		    view.createdAt(),
		    view.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, View view)
	{
		view.updatedAt(new Date());
		bs.bind(view.description(),
			view.ttl(),
			view.updatedAt(),
			view.databaseName(),
			view.tableName(),
			view.name());
	}

	protected View marshalRow(Row row)
	{
		if (row == null) return null;

		View v = new View();
		v.name(row.getString(Columns.NAME));

		Table t = new Table();
		t.database(row.getString(Columns.DATABASE));
		t.name(row.getString(Columns.TABLE));
		v.table(t);

		v.description(row.getString(Columns.DESCRIPTION));
		v.ttl(row.getLong(Columns.TTL));
		v.keys(row.getString(Columns.KEYS));
		v.createdAt(row.getTimestamp(Columns.CREATED_AT));
		v.updatedAt(row.getTimestamp(Columns.UPDATED_AT));
		return v;
	}

	private boolean createDocumentSchema(View view)
    {
		try
		{
			return DOCUMENT_SCHEMA.create(session(), keyspace(), view.toDbTable(), KEY_PARSER.parse(view.keys()));
		}
		catch (KeyDefinitionException e)
		{
			throw new StorageException(e);
		}
    }

	private boolean dropDocumentSchema(Identifier id)
    {
		return DOCUMENT_SCHEMA.drop(session(), keyspace(), id.toDbName());
    }
}
