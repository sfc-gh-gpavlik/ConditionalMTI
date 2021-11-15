create or replace stage CREATE_MTI;

-- Get some good rows:
copy into @CREATE_MTI from (

  select
  	 L_ORDERKEY::string      as L_ORDERKEY
	,L_PARTKEY::string       as L_PARTKEY
	,L_SUPPKEY::string       as L_SUPPKEY
	,L_LINENUMBER::string    as L_LINENUMBER
	,L_QUANTITY::string      as L_QUANTITY
	,L_EXTENDEDPRICE::string as L_EXTENDEDPRICE
	,L_DISCOUNT::string      as L_DISCOUNT
	,L_TAX::string           as L_TAX
	,L_RETURNFLAG::string    as L_RETURNFLAG
	,L_LINESTATUS::string    as L_LINESTATUS
	,L_SHIPDATE::string      as L_SHIPDATE
	,L_COMMITDATE::string    as L_COMMITDATE
	,L_RECEIPTDATE::string   as L_RECEIPTDATE
	,L_SHIPINSTRUCT::string  as L_SHIPINSTRUCT
	,L_SHIPMODE::string      as L_SHIPMODE
	,L_COMMENT::string       as L_COMMENT
    
  from "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."LINEITEM"
union all
  select 'NOT_A_NUMBER', '30674', '5681', '1', '6.00', '9628.02', '0.01', '0.09', 'R', 'F', '1992-06-25', '1992-07-04', '1992-07-02', 'Shipping instructions', 'MAIL', 'Comments' -- Bad integer
union all                                                                           --Bad date
  select '12345', '30674', '5681', '1', '6.00', '9628.02', '0.01', '0.09', 'R', 'F', '1992-06-45', '1992-07-04', '1992-07-02', 'Shipping instructions', 'MAIL', 'Comments' -- Bad date
union all                                                                           --Bad date
  select '12345', '30674', '5681', '1', '6.00', '9628.02', '0.01', '0.09', 'RETURN_FLAG_TOO_LONG', 'F', '1992-06-45', '1992-07-04', '1992-07-02', 'Shipping instructions', 'MAIL', 'Comments' -- String too long

) header = true;



call CREATE_MTI_STATEMENT('@CREATE_MTI', 'UTIL_DB.PUBLIC.SKIP_HEADER', 'TEST', 'PUBLIC', 'LINEITEM');
