/*
    Copyright 2016, Strategic Gains, Inc.

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

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.bson.BSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.orangerhymelabs.helenus.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.helenus.cassandra.DataTypes;
import com.orangerhymelabs.helenus.cassandra.view.ViewDocumentRepository.ViewDocumentStatements;
import com.orangerhymelabs.helenus.exception.InvalidIdentifierException;
import com.orangerhymelabs.helenus.exception.StorageException;
import com.orangerhymelabs.helenus.persistence.Identifier;
import com.orangerhymelabs.helenus.persistence.StatementFactory;

/**
 * Document repositories are unique per document/table and therefore must be cached by table.
 * 
 * @author tfredrich
 * @since 2 Sept 2016
 */
public class ViewDocumentRepository
extends AbstractCassandraRepository<ViewDocument, ViewDocumentStatements>
{
	private static final Logger LOG = LoggerFactory.getLogger(ViewDocumentRepository.class);

	private class Columns
	{
		static final String OBJECT = "object";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	public static class Schema
	{
		private static final String DROP_TABLE = "drop table if exists %s.%s;";
		private static final String CREATE_TABLE = "create table if not exists %s.%s" +
		"(" +
			"%s," +									// identifying properties
		    Columns.OBJECT + " blob," +
			Columns.CREATED_AT + " timestamp," +
		    Columns.UPDATED_AT + " timestamp," +
			"%s" +									// primary key
		")" +
		" %s";										// clustering order (optional)

		public boolean drop(Session session, String keyspace, String table)
        {
			ResultSetFuture rs = session.executeAsync(String.format(DROP_TABLE, keyspace, table));
	        try
	        {
				return rs.get().wasApplied();
			}
	        catch (InterruptedException | ExecutionException e)
	        {
	        	LOG.error("ViewDocument schema drop failed", e);
			}

	        return false;
        }

        public boolean create(Session session, String keyspace, String table, KeyDefinition key)
        {
			ResultSetFuture rs = session.executeAsync(String.format(CREATE_TABLE, keyspace, table, key.asColumns(), key.asPrimaryKey(), key.asClusteringKey()));
			try
			{
				return rs.get().wasApplied();
			}
			catch (InterruptedException | ExecutionException e)
			{
				LOG.error("ViewDocument schema create failed", e);
			}
			
			return false;
        }
	}
	public static class ViewDocumentStatements
	implements StatementFactory
	{
		private KeyDefinition keys;
		private Session session;
		private String keyspace;
		private String tableName;
		private Map<String, PreparedStatement> statements = new ConcurrentHashMap<>();

		public ViewDocumentStatements(Session session, String keyspace, String tableName, KeyDefinition keys)
		throws KeyDefinitionException
		{
			super();
			this.session = session;
			this.keyspace = keyspace;
			this.tableName = tableName;
			this.keys = keys;
		}

		@Override
		public PreparedStatement create()
		{
			PreparedStatement ps = statements.get("create");

			if (ps == null)
			{
				ps = session.prepare(String.format("insert into %s.%s (%s, " + Columns.OBJECT + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (%s) if not exists", keyspace, tableName, keys.asSelectProperties(), keys.asQuestionMarks()));
				statements.put("create", ps);
			}

			return ps;
		}

		@Override
		public PreparedStatement delete()
		{
			PreparedStatement ps = statements.get("delete");

			if (ps == null)
			{
				ps = session.prepare(String.format("delete from %s.%s where %s", keyspace, tableName, keys.asIdentityClause()));
				statements.put("delete", ps);
			}

			return ps;
		}

		public PreparedStatement exists()
		{
			PreparedStatement ps = statements.get("create");

			if (ps == null)
			{
				ps = session.prepare(String.format("select count(*) from %s.%s  where %s limit 1", keyspace, tableName, keys.asIdentityClause()));
				statements.put("create", ps);
			}

			return ps;
		}

		@Override
		public PreparedStatement update()
		{
			PreparedStatement ps = statements.get("update");

			if (ps == null)
			{
				ps = session.prepare(String.format("update %s.%s set %s where %s if exists", keyspace, tableName, keys.asUpdateProperties(), keys.asIdentityClause()));
				statements.put("update", ps);
			}

			return ps;
		}

		public PreparedStatement upsert()
		{
			PreparedStatement ps = statements.get("upsert");

			if (ps == null)
			{
				ps = session.prepare(String.format("insert into %s.%s (%s, " + Columns.OBJECT + ", " + Columns.CREATED_AT + ", " + Columns.UPDATED_AT + ") values (%s)", keyspace, tableName, keys.asSelectProperties(), keys.asQuestionMarks()));
				statements.put("upsert", ps);
			}

			return ps;
		}

		@Override
		public PreparedStatement read()
		{
			PreparedStatement ps = statements.get("read");

			if (ps == null)
			{
				ps = session.prepare(String.format("select * from %s.%s where %s limit 1", keyspace, tableName, keys.asIdentityClause()));
				statements.put("read", ps);
			}

			return ps;
		}

		@Override
		public PreparedStatement readAll()
		{
			PreparedStatement ps = statements.get("readAll");

			if (ps == null)
			{
				ps = session.prepare(String.format("select * from %s.%s where %s", keyspace, tableName, keys.asPartitionIdentityClause()));
				statements.put("readAll", ps);
			}

			return ps;
		}
	}

	private View view;
	private KeyDefinition keys;

	public ViewDocumentRepository(Session session, String keyspace, View view)
	throws KeyDefinitionException
	{
		super(session, keyspace);
		this.view = view;
		this.keys = new KeyDefinitionParser().parse(view.keys());
		statementFactory(new ViewDocumentStatements(session, keyspace, tableName(), keys));
	}

	public String tableName()
	{
		return view.toDbTable();
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
		});
	}

	public ListenableFuture<ViewDocument> upsert(ViewDocument entity)
	{
		ListenableFuture<ResultSet> future = submitUpsert(entity);
		return Futures.transformAsync(future, new AsyncFunction<ResultSet, ViewDocument>()
		{
			@Override
			public ListenableFuture<ViewDocument> apply(ResultSet result)
			throws Exception
			{
				if (result.wasApplied())
				{
					return Futures.immediateFuture(entity);
				}

				//TODO: This doesn't provide any informational value... what should it be?
				return Futures.immediateFailedFuture(new StorageException(String.format("View %s failed to store document: %s", view.toDbTable(), entity.toString())));
			}
		});
	}

	@Override
	protected ResultSetFuture submitCreate(ViewDocument document)
	{
		BoundStatement create = new BoundStatement(statementFactory().create());
		bindCreate(create, document);
		return session().executeAsync(create);
	}

	@Override
	protected ResultSetFuture submitDelete(Identifier id)
	{
		BoundStatement delete = new BoundStatement(statementFactory().delete());
		bindIdentity(delete, id);
		return session().executeAsync(delete);
	}

	protected ListenableFuture<ResultSet> submitExists(Identifier id)
	{
		BoundStatement bs = new BoundStatement(statementFactory().exists());
		bindIdentity(bs, id);
		return session().executeAsync(bs);
	}

	@Override
	protected ResultSetFuture submitUpdate(ViewDocument document)
	{
		BoundStatement update = new BoundStatement(statementFactory().update());
		bindUpdate(update, document);
		return session().executeAsync(update);
	}

	protected ResultSetFuture submitUpsert(ViewDocument document)
	{
		BoundStatement upsert = new BoundStatement(statementFactory().upsert());
		bindCreate(upsert, document);
		return session().executeAsync(upsert);
	}

	@Override
	protected void bindCreate(BoundStatement bs, ViewDocument document)
	{
		Date now = new Date();
		document.createdAt(now);
		document.updatedAt(now);

		try
		{
			if (document.hasObject())
			{
				bindIdentity(bs, document.identifier());
				bs.bind(ByteBuffer.wrap(BSON.encode(document.object())),
				    document.createdAt(),
				    document.updatedAt());
			}
			else
			{
				bindIdentity(bs, document.identifier());
				bs.bind(null,
				    document.createdAt(),
				    document.updatedAt());
			}
		}
		catch (InvalidTypeException | CodecNotFoundException e)
		{
			throw new InvalidIdentifierException(e);
		}
	}

	@Override
	protected void bindUpdate(BoundStatement bs, ViewDocument document)
	{
		document.updatedAt(new Date());

		try
		{
			if (document.hasObject())
			{
				bs.bind(ByteBuffer.wrap(BSON.encode(document.object())),
					document.updatedAt());
				bindIdentity(bs, document.identifier());
			}
			else
			{
				bs.bind(null, document.updatedAt());
				bindIdentity(bs, document.identifier());
			}
		}
		catch (InvalidTypeException | CodecNotFoundException e)
		{
			throw new InvalidIdentifierException(e);
		}
	}

	@Override
	protected ViewDocument marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		ViewDocument d = new ViewDocument();
		d.identifier(marshalId(keys, row));
		ByteBuffer b = row.getBytes(Columns.OBJECT);

		if (b != null && b.hasArray())
		{
			byte[] result = new byte[b.remaining()];
			b.get(result);
			d.object(BSON.decode(result));
		}

		d.createdAt(row.getTimestamp(Columns.CREATED_AT));
		d.updatedAt(row.getTimestamp(Columns.UPDATED_AT));
		return d;
	}

	private Identifier marshalId(KeyDefinition keyDefinition, Row row)
	{
		Identifier id = new Identifier();
		keyDefinition.components().forEach(new Consumer<KeyComponent>()
		{
			@Override
			public void accept(KeyComponent t)
			{
				id.add(marshalIdProperty(t.property(), t.type(), row));
			}
		});
		return id;
	}

	private Object marshalIdProperty(String property, DataTypes type, Row row)
    {
		switch(type)
		{
			case BIGINT: return row.getLong(property);
			case DECIMAL: return row.getDecimal(property);
			case DOUBLE: return row.getDouble(property);
			case FLOAT: return row.getFloat(property);
			case INTEGER: return row.getInt(property);
			case TEXT: return row.getString(property);
			case TIMESTAMP: return row.getTimestamp(property);
			case TIMEUUID:
			case UUID:  return row.getUUID(property);
			default: throw new UnsupportedOperationException("Conversion of property type: " + type.toString());
		}
    }
}
