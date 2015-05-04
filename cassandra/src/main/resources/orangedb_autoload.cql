create keyspace IF NOT EXISTS megadoc
with replication = { 'class' : 'NetworkTopologyStrategy', 'use1' : 2, 'usw2' : 2};
use megadoc;

create table sys_meta (
	id text,
	version text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((id), updated_at)
)
with clustering order by (updated_at DESC);

create table sys_db (
	db_name text primary key,
	description text,
	created_at timestamp,
	updated_at timestamp
);

create table sys_tbl (
	db_name text,
	tbl_name text,
	engine_type int,
	description text,
	tbl_type int,
	tbl_schema text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((db_name), tbl_name)
);

create table sys_idx (
	db_name text,
	tbl_name text,
	name text,
	is_unique boolean,
	fields list<text>,
	only list<text>,
	bucket_sz bigint,
	created_at timestamp,
	updated_at timestamp,
	primary key ((db_name), tbl_name, name)
);
