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

import java.nio.ByteBuffer;

import org.bson.BSON;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.orangerhymelabs.orangedb.cassandra.AbstractCassandraRepository;
import com.orangerhymelabs.orangedb.cassandra.index.FieldType;

/**
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class DocumentRepository
extends AbstractCassandraRepository<Document>
{
	public static class Schema
	{
		private static final String DROP_TABLE = "drop table if exists %s.%s;";
		private static final String CREATE_TABLE = "create table if not exists %s.%s" +
		"(" +
			"id %s," +
		    "object blob," +
			"created_at timestamp," +
		    "updated_at timestamp," +
//			"primary key (id))" +
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

	private class Columns
	{

		static final String ID = "id";
		static final String OBJECT = "object";
		static final String CREATED_AT = "created_at";
		static final String UPDATED_AT = "updated_at";
	}

	private static final String READ_CQL = "select * from %s.%s where id = ?";
	private static final String DELETE_CQL = "delete from %s.%s where id = ?";
	private static final String UPDATE_CQL = "update %s set object = ?, updated_at = ? where id = ? if exists";
	private static final String CREATE_CQL = "insert into %s.%s (id, object, created_at, updated_at) values (?, ?, ?, ?) if not exists";

	private String table;

	public DocumentRepository(Session session, String keyspace, String table)
	{
		super(session, keyspace);
		this.table = table;
	}

	@Override
	protected void bindCreate(BoundStatement bs, Document entity)
	{
		bs.bind(entity.id(),
			ByteBuffer.wrap(BSON.encode(entity.object())),
		    entity.createdAt(),
		    entity.updatedAt());
	}

	@Override
	protected void bindUpdate(BoundStatement bs, Document entity)
	{
		bs.bind(ByteBuffer.wrap(BSON.encode(entity.object())),
			entity.updatedAt(),
		    entity.id());
	}

	@Override
	protected Document marshalRow(Row row)
	{
		if (row == null)
		{
			return null;
		}

		Document d = new Document();
		d.id(row.getUUID(Columns.ID));
		ByteBuffer b = row.getBytes(Columns.OBJECT);

		if (b != null && b.hasArray())
		{
			byte[] result = new byte[b.remaining()];
			b.get(result);
			d.object(BSON.decode(result));
		}

		d.createdAt(row.getDate(Columns.CREATED_AT));
		d.updatedAt(row.getDate(Columns.UPDATED_AT));
		return d;
	}

	@Override
    protected String buildCreateStatement()
    {
	    return String.format(CREATE_CQL, keyspace(), table);
    }

	@Override
    protected String buildUpdateStatement()
    {
	    return String.format(UPDATE_CQL, keyspace(), table);
    }

	@Override
    protected String buildReadStatement()
    {
	    return String.format(READ_CQL, keyspace(), table);
    }

	@Override
    protected String buildReadAllStatement()
    {
	    return null;
    }

	@Override
    protected String buildDeleteStatement()
    {
	    return String.format(DELETE_CQL, keyspace(), table);
    }
}
