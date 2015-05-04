drop keyspace if exists megadoc;
create keyspace megadoc
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
// with replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 };
use megadoc;

drop table if exists sys_meta;
create table sys_meta (
	id text,
	version text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((id), updated_at)
)
with clustering order by (updated_at DESC);

drop table if exists sys_db;
create table sys_db (
	db_name text primary key,
	description text,
	created_at timestamp,
	updated_at timestamp
);

drop table if exists sys_tbl;
create table sys_tbl (
	db_name text,
	tbl_name text,
	description text,
	tbl_type int,
	tbl_schema text,
	created_at timestamp,
	updated_at timestamp,
	primary key ((db_name), tbl_name)
);

drop table if exists sys_idx;
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
