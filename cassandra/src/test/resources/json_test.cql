drop keyspace if exists json_test;
create keyspace if not exists json_test
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
use json_test;

drop table if exists documents;
create table if not exists documents (
	id uuid,
	json text,
	primary key (id)
);

insert into documents (id, json) values (d4e17f0a-90fa-4e4b-8462-484c8ac4f755, '{"firstName":"Dinah","lastName":"Irish","birthYear":1946,"bornOn":"1946/08/07"}');
insert into documents (id, json) values (49d47e71-e008-4672-9fdf-32e288213b90, '{"firstName":"Todd","lastName":"Fredrich","birthYear":1964,"bornOn":"1964/12/17"}');
insert into documents (id, json) values (498bfa4d-7628-4779-a4d6-79a0bff21a2a, '{"firstName":"Leanna","lastName":"Fredrich","birthYear":1963,"bornOn":"1963/12/06"}');
insert into documents (id, json) values (4f1cc787-ce26-489f-b7b4-158b87ae4bbf, '{"firstName":"JP","lastName":"Morgan","birthYear":1925,"bornOn":"1925/01/01"}');
insert into documents (id, json) values (73d01eaf-c65c-4cf0-8904-e1d45831609b, '{"firstName":"Helen","lastName":"Marsh","birthYear":1955,"bornOn":"1955/08/31"}');

drop table if exists indexes;
create table if not exists indexes (
	name text,
	fields list<text>,
	primary key (name)
);

insert into indexes (name, fields) values ('first_name_last_name', ['firstName:text', 'lastName:text']);

drop table if exists first_name_last_name;
create table if not exists first_name_last_name (
	bucket_id int,
	lastname text,
	firstname text,
	id uuid,
	json text,
	primary key ((bucket_id), lastname, firstname)
);

insert into first_name_last_name (id, json, lastname, firstname, bucket_id) values (d4e17f0a-90fa-4e4b-8462-484c8ac4f755, '{"firstName":"Dinah","lastName":"Irish","birthYear":1946,"bornOn":"1946/08/07"}', 'Irish', 'Dinah', 1);
insert into first_name_last_name (id, json, lastname, firstname, bucket_id) values (49d47e71-e008-4672-9fdf-32e288213b90, '{"firstName":"Todd","lastName":"Fredrich","birthYear":1964,"bornOn":"1964/12/17"}', 'Fredrich', 'Todd', 1);
insert into first_name_last_name (id, json, lastname, firstname, bucket_id) values (498bfa4d-7628-4779-a4d6-79a0bff21a2a, '{"firstName":"Leanna","lastName":"Fredrich","birthYear":1963,"bornOn":"1963/12/06"}', 'Fredrich', 'Leanna', 1);
insert into first_name_last_name (id, json, lastname, firstname, bucket_id) values (4f1cc787-ce26-489f-b7b4-158b87ae4bbf, '{"firstName":"JP","lastName":"Morgan","birthYear":1925,"bornOn":"1925/01/01"}', 'Morgan', 'JP', 1);
insert into first_name_last_name (id, json, lastname, firstname, bucket_id) values (73d01eaf-c65c-4cf0-8904-e1d45831609b, '{"firstName":"Helen","lastName":"Marsh","birthYear":1955,"bornOn":"1955/08/31"}', 'Marsh', 'Helen', 1);


drop table if exists birth_year;
create table if not exists birth_year (
	bucket_id int,
	birthyear int,
	id uuid,
	json text,
	primary key ((bucket_id), birthyear)
);

insert into birth_year (id, json, birthyear, bucket_id) values (d4e17f0a-90fa-4e4b-8462-484c8ac4f755, '{"firstName":"Dinah","lastName":"Irish","birthYear":1946,"bornOn":"1946/08/07"}', 1946, 1);
insert into birth_year (id, json, birthyear, bucket_id) values (49d47e71-e008-4672-9fdf-32e288213b90, '{"firstName":"Todd","lastName":"Fredrich","birthYear":1964,"bornOn":"1964/12/17"}', 1964, 1);
insert into birth_year (id, json, birthyear, bucket_id) values (498bfa4d-7628-4779-a4d6-79a0bff21a2a, '{"firstName":"Leanna","lastName":"Fredrich","birthYear":1963,"bornOn":"1963/12/06"}', 1963, 1);
insert into birth_year (id, json, birthyear, bucket_id) values (4f1cc787-ce26-489f-b7b4-158b87ae4bbf, '{"firstName":"JP","lastName":"Morgan","birthYear":1925,"bornOn":"1925/01/01"}', 1925, 1);
insert into birth_year (id, json, birthyear, bucket_id) values (73d01eaf-c65c-4cf0-8904-e1d45831609b, '{"firstName":"Helen","lastName":"Marsh","birthYear":1955,"bornOn":"1955/08/31"}', 1955, 1);

