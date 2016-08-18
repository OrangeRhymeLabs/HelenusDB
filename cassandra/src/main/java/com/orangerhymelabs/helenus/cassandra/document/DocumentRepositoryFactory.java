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

import com.orangerhymelabs.helenus.cassandra.table.Table;

/**
 * @author toddf
 * @since Jun 15, 2015
 */
public interface DocumentRepositoryFactory
{
	public DocumentRepository newDocumentRepositoryFor(Table table);
}