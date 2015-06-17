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
package com.orangerhymelabs.orangedb.cassandra.document;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.orangerhymelabs.orangedb.cassandra.FieldType;
import com.orangerhymelabs.orangedb.cassandra.table.Table;
import com.orangerhymelabs.orangedb.exception.StorageException;
import com.orangerhymelabs.orangedb.persistence.Identifier;

/**
 * Document repositories are unique per document/table and therefore must be cached by table.
 * This repository does not update document, instead creates a new one, keeping the history.
 * It also facilitates adding ACID transactions later when we use 
 * 
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class HistoricalDocumentRepository
extends DocumentRepository
{
	public static class Schema
	{
		private static final String DROP_TABLE = "drop table if exists %s.%s;";
		private static final String CREATE_TABLE = "create table if not exists %s.%s" +
		"(" +
			"id %s," +
		    "object blob," +
		    // TODO: Add Location details to HistoricalDocument.
			"created_at timestamp," +
		    "updated_at timestamp," +
		 	"primary key ((id), updated_at)" +
		 ") with clustering order by (updated_at DESC);";

        public boolean drop(Session session, String keyspace, String table)
        {
			ResultSet rs = session.execute(String.format(DROP_TABLE, keyspace, table));
	        return rs.wasApplied();
        }

        public boolean create(Session session, String keyspace, String table, FieldType idType)
        {
			ResultSet rs = session.execute(String.format(CREATE_TABLE, keyspace, table, idType.cassandraType()));
			return rs.wasApplied();
        }
	}

	private static final String READ_CQL = "select * from %s.%s where id = ? limit 1";
	private static final String READ_HISTORY_CQL = "select * from %s.%s where id = ?";
	private static final String UPDATE_CQL = "insert into %s.%s (id, object, created_at, updated_at) values (?, ?, ?, ?) if not exists";
	private static final String EXISTS_CQL = "select count(*) from %s.%s where id = ? limit 1";

	private PreparedStatement existsStmt;
	private PreparedStatement historyStmt;

	public HistoricalDocumentRepository(Session session, String keyspace, Table table)
	{
		super(session, keyspace, table);
		this.existsStmt = prepare(String.format(EXISTS_CQL, keyspace(), table.name()));
		this.historyStmt = prepare(String.format(READ_HISTORY_CQL, keyspace(), table.name()));
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

	public List<Document> readHistory(Identifier id)
	{
		ResultSet rs;

        try
        {
	        rs = _readHistory(id).get();
	        return (marshalAll(rs));
        }
        catch (InterruptedException | ExecutionException e)
        {
        	throw new StorageException(e);
        }
	}

	public void readHistoryAsync(Identifier id, FutureCallback<List<Document>> callback)
	{
		ResultSetFuture future = _readHistory(id);
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

	private ResultSetFuture _readHistory(Identifier id)
	{
		BoundStatement bs = new BoundStatement(historyStmt);
		bindIdentity(bs, id);
		return session().executeAsync(bs);
	}

	@Override
    protected String buildUpdateStatement()
    {
	    return String.format(UPDATE_CQL, keyspace(), tableName());
    }

	@Override
    protected String buildReadStatement()
    {
	    return String.format(READ_CQL, keyspace(), tableName());
    }
}
