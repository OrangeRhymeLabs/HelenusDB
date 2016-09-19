package com.orangerhymelabs.helenus.cassandra.document;

import com.orangerhymelabs.helenus.cassandra.table.Table;
import com.orangerhymelabs.helenus.cassandra.table.key.KeyDefinitionException;

public interface DocumentRepositoryFactory
{
	DocumentRepository newInstance(Table t)
	throws KeyDefinitionException;
}
