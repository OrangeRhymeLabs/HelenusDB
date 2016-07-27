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
package com.orangerhymelabs.helenus.cassandra.index;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.SchemaProvider;
import com.orangerhymelabs.helenus.cassandra.bucket.BucketedViewStatementFactory;
import com.orangerhymelabs.helenus.exception.DuplicateItemException;
import com.orangerhymelabs.helenus.exception.ItemNotFoundException;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class IndexRepository
extends AbstractCassandraRepository<Index>
{
	private static final Logger LOG = LoggerFactory.getLogger(IndexRepository.class);

	private class Tables
	{
		static final String BY_ID = "sys_idx";
	}

	private class Columns
	{
		static final String DB_NAME = "db_name";
		static final String TBL_NAME = "tbl_name";
		static final String NAME = "name";
		static final String DESCRIPTION = "description";
		static final String IS_UNIQUE = "is_unique";
		static final String IS_CASE_SENSISTIVE = "is_case_sensitive";
		static final String FIELDS = "fields";
		static final String CONTAINS_ONLY = "contains_only";
		static final String ID_TYPE = "id_type";
		static final String ENGINE = "engine";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	public static class Schema
	implements SchemaProvider
	{
		private static final String DROP_TABLE = "drop table if exists %s." + Tables.BY_ID;
		private static final String CREATE_TABLE = "create table if not exists %s." + Tables.BY_ID +
			"(" +
				Columns.DB_NAME +				" text," +
				Columns.TBL_NAME +				" text," +
				Columns.NAME +					" text," +
				Columns.DESCRIPTION +			" text," +
				Columns.FIELDS +				" list<text>," +
				Columns.CONTAINS_ONLY +			" list<text>," +
				Columns.ID_TYPE +				" text," +
				Columns.IS_UNIQUE +				" boolean," +
				Columns.IS_CASE_SENSISTIVE +	" boolean," +
				Columns.ENGINE +				" text," +
				Columns.CREATED_AT +			" timestamp," +
				Columns.UPDATED_AT +			" timestamp," +
				"primary key ((" + Columns.DB_NAME + "), " + Columns.TBL_NAME + ", " + Columns.NAME + ")" +
			")";

		@Override
		public boolean drop(Session session, String keyspace)
		{
			ResultSetFuture rs = session.executeAsync(String.format(Schema.DROP_TABLE, keyspace));
			try
			{
				return rs.get().wasApplied();
			}
			catch (InterruptedException | ExecutionException e)
			{
				LOG.error("Index schema drop failed", e);
			}

			return false;
		}

		@Override
		public boolean create(Session session, String keyspace)
		{
			ResultSetFuture rs = session.executeAsync(String.format(Schema.CREATE_TABLE, keyspace));
			try
			{
				return rs.get().wasApplied();
			}
			catch (InterruptedException | ExecutionException e)
			{
				LOG.error("Index schema create failed", e);
			}

			return false;
		}
	}

	private static final String IDENTITY_CQL = " where " + Columns.DB_NAME + "= ? and " + Columns.TBL_NAME + " = ? and " + Columns.NAME + " = ?";
	private static final String CREATE_CQL = "insert into %s.%s ("
		+ Columns.DB_NAME + ", "
		+ Columns.TBL_NAME + ", "
		+ Columns.NAME + ", "
		+ Columns.DESCRIPTION + ", "
		+ Columns.FIELDS + ", "
		+ Columns.CONTAINS_ONLY + ", "
		+ Columns.ID_TYPE + ", "
		+ Columns.IS_UNIQUE + ", "
		+ Columns.IS_CASE_SENSISTIVE + ", "
		+ Columns.ENGINE + ", "
		+ Columns.CREATED_AT + ", "
		+ Columns.UPDATED_AT
		+ ") values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) if not exists";
	private static final String UPDATE_CQL = "update %s.%s set "
		+ Columns.DESCRIPTION + " = ?, "
		+ Columns.UPDATED_AT + " = ?"
		+ IDENTITY_CQL
		+ " if exists";
	private static final String READ_CQL = "select * from %s.%s" + IDENTITY_CQL;
	private static final String READ_FOR_TABLE_CQL = "select * from %s.%s where " + Columns.DB_NAME + "= ? and " + Columns.TBL_NAME + " = ?";
	private static final String READ_ALL_CQL = "select * from %s.%s";
	private static final String DELETE_CQL = "delete from %s.%s" + IDENTITY_CQL;

	private static final BucketedViewStatementFactory.Schema BUCKETED_VIEW_SCHEMA = new BucketedViewStatementFactory.Schema();

	private PreparedStatement readForTableStmt;

	public IndexRepository(Session session, String keyspace)
	{
		super(session, keyspace);
		readForTableStmt = prepare(String.format(READ_FOR_TABLE_CQL, keyspace(), Tables.BY_ID));
	}

	@Override
	public ListenableFuture<Index> create(Index index)
	{
		optionallyCreateViewTable(index);
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
		}, MoreExecutors.newDirectExecutorService());
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
		BUCKETED_VIEW_SCHEMA.drop(session(), keyspace(), id.toDbName());
		super.delete(id);
	}

	@Override
	public void deleteAsync(Identifier id, FutureCallback<Index> callback)
	{
		try
		{
			BUCKETED_VIEW_SCHEMA.drop(session(), keyspace(), id.toDbName());
			super.deleteAsync(id, callback);
		}
		catch(AlreadyExistsException e)
		{
			callback.onFailure(new ItemNotFoundException(e));
		}
		catch(Exception e)
		{
			callback.onFailure(new StorageException(e));
		}
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

		if (index.isBucketedView())
		{
			BucketedViewIndex bvi = (BucketedViewIndex) index;
			bs.bind(bvi.databaseName(),
				bvi.tableName(),
				bvi.name(),
				bvi.description(),
				bvi.fields(),
				bvi.containsOnly(),
				bvi.idType().name(),
				bvi.isUnique(),
				bvi.isCaseSensitive(),
				bvi.engine().name(),
				bvi.createdAt(),
				bvi.updatedAt());
		}
		else if (index.isExternal())
		{
			LuceneIndex li = (LuceneIndex) index;
			bs.bind(li.databaseName(),
				li.tableName(),
				li.name(),
				li.description(),
				null,
				null,
				li.idType().name(),
				null,
				null,
				li.engine().name(),
				li.createdAt(),
				li.updatedAt());
		}
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

		IndexEngine engine = IndexEngine.valueOf(row.getString(Columns.ENGINE));

		if (IndexEngine.BUCKETED_VIEW.equals(engine))
		{
			return marshalBucketedViewIndex(row);
		}
		else if (IndexEngine.LUCENE.equals(engine))
		{
			return marshalExternalIndex(row);
		}

		return null;
	}

	private Index marshalBucketedViewIndex(Row row)
	{
		BucketedViewIndex n = new BucketedViewIndex();
		n.table(row.getString(Columns.DB_NAME), row.getString(Columns.TBL_NAME), DataTypes.from(row.getString(Columns.ID_TYPE)));
		n.name(row.getString(Columns.NAME));
		n.description(row.getString(Columns.DESCRIPTION));
		n.fields(row.getList(Columns.FIELDS, String.class));
		n.containsOnly(row.getList(Columns.CONTAINS_ONLY, String.class));
		n.isUnique(row.getBool(Columns.IS_UNIQUE));
		n.isCaseSensitive(row.getBool(Columns.IS_CASE_SENSISTIVE));
		n.createdAt(row.getTimestamp(Columns.CREATED_AT));
		n.updatedAt(row.getTimestamp(Columns.UPDATED_AT));
		return n;
	}

	private Index marshalExternalIndex(Row row)
	{
		IndexEngine engine = IndexEngine.valueOf(row.getString(Columns.ENGINE));
		Index n = constructIndexType(engine);
		n.table(row.getString(Columns.DB_NAME), row.getString(Columns.TBL_NAME), DataTypes.from(row.getString(Columns.ID_TYPE)));
		n.name(row.getString(Columns.NAME));
		n.description(row.getString(Columns.DESCRIPTION));
		n.createdAt(row.getTimestamp(Columns.CREATED_AT));
		n.updatedAt(row.getTimestamp(Columns.UPDATED_AT));
		return n;
	}

	private Index constructIndexType(IndexEngine engine)
	{
		if (IndexEngine.ELASTIC_SEARCH.equals(engine))
		{
			return new ElasticSearchIndex();
		}
		else if (IndexEngine.LUCENE.equals(engine))
		{
			return new LuceneIndex();
		}
		else if (IndexEngine.SOLR.equals(engine))
		{
			return new SolrIndex();
		}

		throw new StorageException("Invalid engine type: " + engine.name());
	}

	private void optionallyCreateViewTable(Index index)
	{
		if (index.isBucketedView())
		{
			BucketedViewIndex bvi = (BucketedViewIndex) index;
			BUCKETED_VIEW_SCHEMA.create(session(), keyspace(), bvi.toDbTable(), bvi.idType(), bvi.toColumnDefs(), bvi.toPkDefs(), bvi.toClusterOrderings());
		}
	}
}
