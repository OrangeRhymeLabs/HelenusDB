INSERT OR REPLACE INTO config (serverFunction, name, value) values ('A','port','8081');
INSERT OR REPLACE INTO config (serverFunction, name, value) values ('A','executor.threadPool.size','20');
INSERT OR REPLACE INTO config (serverFunction, name, value) values ('A','cassandra.keyspace','docussandra');
INSERT OR REPLACE INTO config (serverFunction, name, value) values ('A','cassandra.readConsistencyLevel','LOCAL_QUORUM');
INSERT OR REPLACE INTO config (serverFunction, name, value) values ('A','cassandra.writeConsistencyLevel','LOCAL_QUORUM');
INSERT OR REPLACE INTO config (serverFunction, name, value) values ('A','metrics.isEnabled','false');
