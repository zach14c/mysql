eval
create table product (
  category int not null,
  id int not null,
  price decimal,
  primary key(category, id))
engine=$engine;

eval
create table customer (
  id int not null,
  primary key (id))
engine=$engine;

eval
create table product_order (
  no int not null auto_increment,
  product_category int not null,
  product_id int not null,
  customer_id int not null,
  primary key(no),
  index (product_category, product_id),
  foreign key fk1 (product_category, product_id) references product(category, id)
    on update restrict on delete cascade,
  index (customer_id),
  foreign key fk2 (customer_id) references customer(id))
engine=$engine;

eval
create table emp (
  id int primary key auto_increment,
  manager int,
  key (manager),
  foreign key fk1 (manager) references emp (id))
engine=$engine;

select *
from information_schema.table_constraints
where table_schema = 'test'
order by table_name, constraint_type, constraint_name;

--disable_query_log
select CONCAT_WS('.', TABLE_NAME, COLUMN_NAME) as COL,
       ORDINAL_POSITION,
       POSITION_IN_UNIQUE_CONSTRAINT,
       CONCAT_WS('.', REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME) as REFERENCED_COL
from information_schema.key_column_usage
where table_schema = 'test'
order by COL, ORDINAL_POSITION;
--enable_query_log

drop table product_order, customer, product, emp;