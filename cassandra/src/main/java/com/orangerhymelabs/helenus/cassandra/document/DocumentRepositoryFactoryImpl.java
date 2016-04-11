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
package com.orangerhymelabs.helenus.cassandra.document;

import com.datastax.driver.core.Session;
import com.orangerhymelabs.helenus.cassandra.index.IndexRepository;
import com.orangerhymelabs.helenus.cassandra.table.Table;

/**
 * @author tfredrich
 * @since Jun 23, 2015
 */
public class DocumentRepositoryFactoryImpl
implements DocumentRepositoryFactory
{
	private Session session;
	private String keyspace;
	private IndexRepository indexes;

	public DocumentRepositoryFactoryImpl(Session session, String keyspace, IndexRepository indexRepository)
	{
		super();
		this.session = session;
		this.keyspace = keyspace;
		this.indexes = indexRepository;
	}

	@Override
	public DocumentRepository newDocumentRepositoryFor(Table table)
	{
		table.indexes(indexes.readFor(table.databaseName(), table.name()));
		return new DocumentRepository(session, keyspace, table);
	}
}
