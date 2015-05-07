package com.orangerhymelabs.orangedb.cassandra.table;

/**
 * MegaDoc supports four table types:
 * DOCUMENT (default, if not specified) - indexable schema-less BSON documents.
 * SCHEMA - indexable schema-oriented tables.
 * COUNTER - Cassandra-based named counters.
 * TIME_SERIES - Cassandra-based time-series data.
 * 
 * @author toddf
 * @since May 2, 2015
 */
public enum TableType
{
	DOCUMENT,
	SCHEMA,
	COUNTER,
	TIME_SERIES;

	public static TableType valueOf(int ordinal)
	{
		return (TableType.values()[ordinal]);
	}
}
