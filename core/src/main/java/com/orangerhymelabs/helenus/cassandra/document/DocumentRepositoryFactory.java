package com.orangerhymelabs.helenus.cassandra.document;

import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;
import com.strategicgains.noschema.document.View;

public interface DocumentRepositoryFactory
{
	AbstractDocumentRepository newInstance(Table t)
	throws KeyDefinitionException;

	AbstractDocumentRepository newInstance(View v)
	throws KeyDefinitionException;
}
