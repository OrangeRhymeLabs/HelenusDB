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
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.strategicgains.noschema.document.View;

/**
 * Document repositories are unique per document/table and therefore must be cached by table.
 * 
 * @author tfredrich
 * @since Jun 8, 2015
 */
public class ViewDocumentRepository
extends AbstractDocumentRepository
{
	public ViewDocumentRepository(Session session, String keyspace, View view)
	throws KeyDefinitionException
	{
		super(session, keyspace, view.toDbTable(), view.keys());
	}
}
