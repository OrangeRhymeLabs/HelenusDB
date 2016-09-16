package com.orangerhymelabs.helenus.cassandra.view;

import com.orangerhymelabs.helenus.cassandra.view.key.KeyDefinitionException;

public interface ViewDocumentRepositoryFactory
{
	ViewDocumentRepository newInstance(View view)
	throws KeyDefinitionException;
}
