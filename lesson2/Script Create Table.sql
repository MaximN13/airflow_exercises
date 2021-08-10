create table if not exists
table_info (
 name varchar(128),
 age integer,
 sku_name varchar(512),
 date varchar(32),
 payment_status varchar(512),
 total_price decimal,
 last_modified_dt varchar(32)
);

truncate table table_info;

--test
insert into table_info(
		name ,
 		age ,
 		sku_name ,
 		date ,
 		payment_status ,
 		total_price ,
 		last_modified_dt 
		)
values (
		'name',
		14,
		'sku_name',
		'2021-12-21',
		'success_status',
		123.12,
		'2021-12-01'
		);
commit;
