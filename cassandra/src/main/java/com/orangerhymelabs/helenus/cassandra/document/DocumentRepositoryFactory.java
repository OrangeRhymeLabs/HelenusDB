package com.orangerhymelabs.helenus.cassandra.document;

import com.orangerhymelabs.helenus.cassandra.table.Table;

public interface DocumentRepositoryFactory
{
	DocumentRepository newInstance(Table t);
}
