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
import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.view.key.KeyDefinitionException;

/**
 * @author tfredrich
 * @since Jun 23, 2015
 */
public class DocumentRepositoryFactoryImpl
implements DocumentRepositoryFactory
{
	private Session session;
	private String keyspace;

	public DocumentRepositoryFactoryImpl(Session session, String keyspace)
	{
		super();
		this.session = session;
		this.keyspace = keyspace;
	}

	@Override
	public DocumentRepository newInstance(Table table)
	throws KeyDefinitionException
	{
		return new DocumentRepository(session, keyspace, table);
	}
}
