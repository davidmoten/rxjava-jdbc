create table person (name varchar(50) primary key, score int not null,dob date, registered timestamp);
insert into person(name,score) values('FRED',21);
insert into person(name,score) values('JOSEPH',34);
insert into person(name,score) values('MARMADUKE',25);
create table person_clob (name varchar(50) not null,  document clob not null);
create table person_blob (name varchar(50) not null, document blob not null);
