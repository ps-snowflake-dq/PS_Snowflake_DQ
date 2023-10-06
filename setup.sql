

--create metadata tables
create sequence DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ START 1 INCREMENT 1;
create sequence DATA_GOV_METADATA_APP.DQ_VIOLATIONS_SEQ START 1 INCREMENT 1;

create sequence DATA_GOV_METADATA_APP.DQ_RULES_SEQ START 1 INCREMENT 1;

create sequence DATA_GOV_METADATA_APP.PROCESS_METADATA_SEQ START 1 INCREMENT 1;

create or replace TABLE DATA_GOV_METADATA_APP.DATASETS (
	ID NUMBER(38,0),
	VERSION VARCHAR(16777216),
	DB VARCHAR(16777216),
	SCHEMA VARCHAR(16777216),
	TABLE_NAME VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	DOMAIN VARCHAR(16777216),
	SOURCE VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_NTZ(9),
	UPDATED_BY VARCHAR(16777216)
);


create or replace table DATA_GOV_METADATA_APP.ATTRIBUTES(
Id NUMBER,
version VARCHAR,
table_id NUMBER, 
DB VARCHAR,
Schema VARCHAR, 
Table_name VARCHAR, 
Column_name VARCHAR,
Data_type VARCHAR, 
Length NUMBER,
trust_score NUMBER DEFAULT NULL,
Mandatory VARCHAR, 
Unique_ VARCHAR,
PII VARCHAR,
PD VARCHAR,
Description VARCHAR, 
Insert_date TIMESTAMP,
Updated_by VARCHAR 
);

create or replace TABLE DATA_GOV_METADATA_APP.DQ_RULES (
	ID NUMBER(38,0),
	COLUMN_ID NUMBER(38,0),
	RULENAME VARCHAR(16777216),
	RULE_EXPRESSION VARCHAR(16777216),
	RULE_DESCRIPTION VARCHAR(16777216),
	START_DATE TIMESTAMP_NTZ(9),
	END_DATE TIMESTAMP_NTZ(9),
	IS_ACTIVE BOOLEAN,
	INSERT_DATE TIMESTAMP_NTZ(9),
	UPDATE_DATE TIMESTAMP_NTZ(9),
	CREATED_BY VARCHAR(16777216),
	UPDATED_BY VARCHAR(16777216)
);

create or replace TABLE DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP (
	ID NUMBER(38,0),
	RULENAME VARCHAR(16777216),
	GE_FUNCTIONNAME VARCHAR(16777216),
	SAMPLE_EXPRESSION VARCHAR(16777216),
	INSERT_DATE TIMESTAMP_NTZ(9),
	INSERT_BY VARCHAR(16777216)
);

create or replace TABLE DATA_GOV_METADATA_APP.DQ_VIOLATIONS (
	ID NUMBER(38,0),
	RUN_ID VARCHAR(16777216),
	RULE_ID VARCHAR(16777216),
	RUNDATETIME TIMESTAMP_NTZ(9),
	COMMENTS VARCHAR(16777216),
	ROW_SCANNED NUMBER(38,0),
	FAILED_COUNT NUMBER(38,0),
	FAILED_PERCENTAGE NUMBER(38,0),
	FAILED_RECORDS VARIANT,
	INSERT_TIMESTAMP TIMESTAMP_NTZ(9)
);

create or replace TABLE DATA_GOV_METADATA_APP.SENSITIVE_ATTRIBUTES (
	ID NUMBER(38,0),
	ATTRIBUTE VARCHAR(16777216),
	PII VARCHAR(16777216),
	PD VARCHAR(16777216)
);

create or replace TABLE DATA_GOV_METADATA_APP.PROCESS_METADATA (
	PIPELINE_ID NUMBER(38,0),
	SESSIONID NUMBER(38,0),
	RUNDATE TIMESTAMP_NTZ(9),
	SOURCE_TABLE VARCHAR(16777216),
	TARGET_TABLE VARCHAR(16777216),
	RECORDSPROCESSED NUMBER(38,0),
	FILESPROCESSED VARCHAR(16777216),
	RECORDSLOADED NUMBER(38,0),
	TIMETAKEN NUMBER(38,0)
);

--DML Insert initial data


insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_value_length_between','expect_column_value_lengths_to_be_between(arg1,arg2,arg3)','value between 19 and 22',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'table_columns_match_set','expect_table_columns_to_match_set(arg1,arg2)','value matches [1,2,3]',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_value_length_between','expect_column_value_lengths_to_be_between(arg1,arg2,arg3)','value between 19 and 22',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'table_columns_match_set','expect_table_columns_to_match_set(arg1,arg2)','value matches [1,2,3]',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'table_row_count_between','expect_table_row_count_to_be_between(arg1,arg2,arg3)','value between 19 and 22',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_be_unique','expect_column_values_to_be_unique(arg1)','NA',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_to_not_be_null','expect_column_values_to_not_be_null(arg1)','NA',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_to_be_type','expect_column_values_to_be_of_type(arg1,arg2)','values type int',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_to_be_list','expect_column_values_to_be_in_type_list(arg1,arg2)','values in [1,2,3]',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_be_between','expect_column_values_to_be_between(arg1,arg2,arg3)','value between 19 and 22',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_increasing','expect_column_values_to_be_increasing(arg1)','NA',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_decreasing','expect_column_values_to_be_decreasing(arg1)','NA',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_value_length_between','expect_column_value_lengths_to_be_between(arg1,arg2,arg3)','value between 19 and 22',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_value_length_equal','expect_column_value_lengths_to_equal(arg1,arg2)','value equals 20',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_match_regex','expect_column_values_to_match_regex(arg1,arg2)','value matches <regex>',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_match_strtime_format','expect_column_values_to_match_strftime_format(arg1,arg2)','value matches dd:mm:yy',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_be_json_parseable','expect_column_values_to_be_json_parseable(arg1,arg2)','value matches {a:b,c:d}',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_match_json_schema','expect_column_values_to_match_json_schema(arg1,arg2)','value matches {a:b,c:d}',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_mean_between','expect_column_mean_to_be_between(arg1,arg2,arg3)','value between 19 and 22',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_sum_between','expect_column_sum_to_be_between(arg1,arg2,arg3)','value between 19 and 22',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_exist','expect_column_to_exist(arg1)','NA',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'table_columns_match_ordered_list','expect_table_columns_to_match_ordered_list(arg1,arg2)','value matches [1,2,3]',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'table_column_count_between','expect_table_column_count_to_be_between(arg1,arg2,arg3)','value between 19 and 22',CURRENT_TIMESTAMP(),current_user());
insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'table_column_count_equal','expect_table_column_count_to_equal(arg1,arg2)','value equals 19',CURRENT_TIMESTAMP(),current_user());

insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_be_valid_date','expect_column_values_to_match_strftime_format(arg1,arg2)','%d-%m-%Y',CURRENT_TIMESTAMP(),current_user());

insert into DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP values (DATA_GOV_METADATA_APP.GE_FUNCTION_RULE_MAP_SEQ.NEXTVAL,'column_values_match_strftime_date','expect_column_values_to_match_strftime_format(arg1,arg2)','%d-%m-%Y',CURRENT_TIMESTAMP(),current_user());

--create procedures to pull metadata
CREATE OR REPLACE PROCEDURE DATA_GOV_METADATA_APP.create_metadata(db VARCHAR, schema VARCHAR, tbl_name VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_metadata'
AS
$$

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import snowflake.snowpark.session


def pii_pd_attributes(session):
    df = session.sql("select * from sensitive_attributes")
    df.show()
    return df.select("ATTRIBUTE", "PII", "PD")


def dataset_versioning(session, dataset_id):
    get_version_query = f"""select max(version) as V from DATASETS where exists(select 1 from DATASETS where id = {dataset_id}) """
    df = session.sql(get_version_query)
    version = "V1.0"
    
    if df.count() > 0:
        print(type(df.select('V').collect()[0][0]))
        version = df.select('V').collect()[0][0]
        version = float(version[1:]) + 1.0
        version = "V" + str(version)
    return version


def populate_attributes_table(session, df):
    count = df.count()
    i = 0

    pii_pd_df = pii_pd_attributes(session)
    while (i < count):
        df.show()
        
        df_1 = df.select("ID")
        df_1.show()
        
        Id = int(df.select("ID").collect()[i][0])
        version = df.select("VERSION").collect()[i][0]
        table_id = int(df.select("ID").collect()[i][0])

        Table_name = df.select("TABLE_NAME").collect()[i][0]
        Column_name = df.select("COLUMN_NAME").collect()[i][0]
        col_id = [ord(i) for i in Column_name]
        tab_id = [ord(i) for i in Table_name]
        col_id = sum(col_id) + sum(tab_id)
        DB = df.select("DB").collect()[i][0]
        Schema = df.select("SCHEMA").collect()[i][0]

        Data_type = df.select("DATA_TYPE").collect()[i][0]
        Length = str(df.select("CHARACTER_MAXIMUM_LENGTH").collect()[i][0])
        if Length == 'None':
            Length = 0
        

        Mandatory = df.select("IS_NULLABLE").collect()[i][0]
        Unique_ = 'No'
        PII = "No"
        PD = "No"
        pii_df = pii_pd_df.select("ATTRIBUTE", "PII").filter(col("PII") == "Y")
        pd_df = pii_pd_df.select("ATTRIBUTE", "PD").filter(col("PD") == "Y")
        pii_list = [pii_df.select("ATTRIBUTE").collect()[_][0].lower() for _ in range(pii_df.count())]
        pd_list = [pii_df.select("ATTRIBUTE").collect()[_][0].lower() for _ in range(pii_df.count())]
        
        pii_list_status = [True if _ in Column_name.lower() else False for _ in pii_list]
        pd_list_status = [True if _ in Column_name.lower() else False for _ in pd_list]
        if any(pii_list_status):
            PII = "Yes"
        if any(pd_list_status):
            PD = "Yes"

        Description = df.select("DESCRIPTION").collect()[i][0]
        insert_query = f"""insert into ATTRIBUTES(Id, version, table_id, DB, Schema, Table_name, Column_name, Data_type, Length, Mandatory, Unique_, PII, PD, Description, Insert_date, Updated_by) values({col_id}, '{version}', {table_id}, '{DB}', '{Schema}', '{Table_name}', '{Column_name}', '{Data_type}', '{Length}', '{Mandatory}', '{Unique_}', '{PII}', '{PD}', '{Description}', current_timestamp(),  current_user()) """
        
        df1 = session.sql(insert_query)
        df1.show()

        i += 1

    df.show()


def create_metadata(session, db_name, schema_name, tbl_name):
    try:
        db = db_name.replace('"', '')
        schema = ""
        if schema_name != None:
            schema = schema_name.replace('"', '')
        else:
            schema = schema_name
        tbl = ""
        tbl_ord= ""
        if tbl_name != None:
            tbl = tbl_name.replace('"', '')
            tbl_ord = tbl
        else:
            tbl_ord= db
            tbl = tbl_name
        role = session.get_current_role().replace('"', '')
        
        dataset_id = [ord(i) for i in tbl_ord]
        dataset_id = sum(dataset_id)

        # Uncommet for versioning
        # version= dataset_versioning(session, dataset_id)
        version = "V1.0".replace('"', '')
        query = f"""select * from (select {dataset_id} as id,'{version}' as version, '{db}' as DB, coalesce('{schema}',TABLE_SCHEMA) as schema, coalesce('{tbl}',TABLE_NAME) as table_name,'default'  as Description,'Default Domain' as domain, 'Source' as source,current_date()  as Inserted_date, 'role' as Inserted_by 
			 from {db}.information_schema.tables where TABLE_SCHEMA = coalesce('{schema}',TABLE_SCHEMA) and 
			TABLE_NAME = coalesce('{tbl}',TABLE_NAME)) """
        
        df = session.sql(query)
        df.show()
        datasets_df = df.drop("COLUMN_NAME", "data_type", "CHARACTER_MAXIMUM_LENGTH", "IS_NULLABLE", "ROW_NUM")
        datasets_df.show()
        # tot_cols = df.select('MAX_ORD').collect()[0][0]
        # Print a sample of the dataframe to standard output.
        datasets_df.write.mode("append").save_as_table("DATASETS")

        attr_data_query = f"""select {dataset_id} as id,'{version}' as version, '{db}' as DB, coalesce('{schema}',TABLE_SCHEMA) as Schema, coalesce('{tbl}',TABLE_NAME) as table_name,'default'  as Description,'Default Domain' as domain, 'Source' as source,current_date()  as Inserted_date, '{role}' as Inserted_by,COLUMN_NAME,data_type,CHARACTER_MAXIMUM_LENGTH,IS_NULLABLE 
				from {db}.information_schema.columns where TABLE_SCHEMA = coalesce('{schema}',TABLE_SCHEMA) and 
				TABLE_NAME = coalesce('{tbl}',TABLE_NAME) order by ordinal_position"""
        
        df = session.sql(attr_data_query)
        datasets_df = df.drop("ROW_NUM")
        populate_attributes_table(session, datasets_df)
        

        return "Metadata created!"

    except Exception as e:
        return "Exception Occured " + str(e)

$$;

--DROP PROCEDURE create_dq_rule(VARCHAR,VARCHAR,VARCHAR);
CREATE OR REPLACE PROCEDURE DATA_GOV_METADATA_APP.create_dq_rule(table_name varchar,column_name varchar,rule_id varchar,expression varchar)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'create_dq_rule'
AS
$$

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import snowflake.snowpark.session

def parse_expression(db_objects,session):
    expression = db_objects[5]
    
    get_integers=[int(_) for _ in expression.split(" ") if _.isdigit()]
    get_string=[_ for _ in expression.split(" ") if _.isalpha() and _.lower()!='and']
    
    get_string = "_".join(get_string)
    
    return get_string,get_integers

def update_dq_rules_table(db_objects,session,get_string,get_integers):
    col_id = [ord(i) for i in db_objects[2]]
    tab_id = [ord(i) for i in db_objects[3]]
    col_id = str(sum(col_id) + sum(tab_id))
    insert_query = f"""insert into DQ_RULES values (DQ_RULES_SEQ.NEXTVAL,{col_id},'{db_objects[4]}','{db_objects[5]}','{get_string}',CURRENT_TIMESTAMP(),dateadd(year,976,current_timestamp()),'Yes',CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP(),CURRENT_USER(),CURRENT_USER());"""
   
    
    df=session.sql(insert_query)
    df.show()

def create_dq_rule(session, table_name, column_name, rule_id, expression):
    
    try:
        db = session.get_current_database().replace('"', '')
        schema = session.get_current_schema().replace('"', '')
        column = column_name.replace('"', '')
        rule_id = rule_id.replace('"', '')
        table_name = table_name.replace('"', '')
        expression = expression
        # rundqrules(db,schema,table)
        db_objects = []
        db_objects.append(db)
        db_objects.append(schema)
        db_objects.append(table_name)
        db_objects.append(column)
        db_objects.append(rule_id)
        db_objects.append(expression)

        # dict_rules not required. Function can be removed as used in expeactaition1

        get_string, get_integers = parse_expression(db_objects, session)

        # Update DQ RUles table
        update_dq_rules_table(db_objects, session, get_string, get_integers)

        return "Rule Created!!"
    except Exception as e:
        return "Exception Occured : " + str(e)

$$;

CREATE OR REPLACE PROCEDURE DATA_GOV_METADATA_APP.rundq_rules(db VARCHAR, schema VARCHAR, tbl_name VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'pandas','great-expectations')
HANDLER = 'main'
AS
$$

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd
import great_expectations as gx
import random

def main(session: snowpark.Session,db_name,schema_name,table_name): 
    session.add_packages("snowflake-snowpark-python", "pandas")
    #session.add_import("great-expectations")
    #try:
    

    def parse_expression(expression):
        expression = expression + " "
        translation_table = str.maketrans('', '','abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~')
        numeric_string = expression.translate(translation_table)
        words = numeric_string.split()
        get_integers = [int(i) for i in words]
        get_string=[_ for _ in expression.split(" ") if _.isalpha() and _.lower()!='and']
        get_string = "_".join(get_string)
        return get_string,get_integers

    def update_trust_score(session,rules_df_col,rules_df_tbl,failed_percentage):
        print("Inside trust score !!")
        trust_score_map={"range(80,100)":1,
                         "range(50,80)":2,
                         "range(25,50)":3,
                         "range(5,25)":4,
                         "range(0,5)":5}
        trust_score = 1
        for key,value in trust_score_map.items():
            if failed_percentage in eval(key):
                trust_score = value
        print(trust_score)
    
        update_query = f"""update attributes set trust_score ={trust_score} where column_name = '{rules_df_col}' and table_name ='{rules_df_tbl}'"""
    
        df=session.sql(update_query)
        df = session.sql("commit;")
    
    
    def great_expectation_dq_check(session,master_df,rules_df_arg):
        print("Starting Data Quality Rules")
        function_map = get_function_rule_map(session)
        print(master_df)
        rules_df_arg.show()
        data_df=master_df
        rules_count = rules_df_arg.count()
        function_name = ""
        get_columns = data_df.columns
        get_columns = ["'" + elem + "'" for elem in get_columns]
        arg1 = ""
        arg2 = ""
        arg3 = ""
        runid = str(random.randint(800011, 19123123))
        rules_df=rules_df_arg
        for _ in range(rules_count):
            rules_df_col = rules_df.select("COLUMN_NAME").collect()[_][0]
            rules_df_tbl = rules_df.select("TABLE_NAME").collect()[_][0]
            rule_df_col_type = rules_df.select("DATA_TYPE").collect()[_][0]
            rule_df_rules = rules_df.select("RULENAME").collect()[_][0]
            rule_df_rule_exp = rules_df.select("RULE_EXPRESSION").collect()[_][0]
            get_string, get_integers = parse_expression(rule_df_rule_exp)
    
            result_format = """result_format = {"result_format": "COMPLETE", "unexpected_index_column_names":""" + f"""["{get_columns}"]""" + """, "include_unexpected_rows": True}"""
            function_name = ""
            print(function_map)
            if rule_df_rules in function_map:
                function_name = function_map[rule_df_rules]
                tot_args_count = len(function_name.split(","))
                if tot_args_count==3:
                    arg2=str(get_integers[0])
                    arg3=str(get_integers[1])
                    arg1= "'" +rules_df_col+"'"
                    arg2= "int(" + arg2 + ")"
                    arg3= "int(" + arg3 + ")," + result_format
                    function_name = function_name.replace("arg1",arg1)
                    function_name = function_name.replace("arg2",arg2)
                    function_name = function_name.replace("arg3",arg3)
    
                elif tot_args_count==2 and "NA" not in rule_df_rule_exp:
                    if "date" in rule_df_rules.lower():
                        arg2="'" + rule_df_rule_exp + "'," + result_format
    
                    else:
                        arg2=str(get_integers[0])
                        arg2 = "int(" + arg2 + ")," + result_format
                    arg1= "'" +rules_df_col+"'"
    
                    function_name = function_name.replace("arg1",arg1)
                    function_name = function_name.replace("arg2", arg2)
    
                else:
                    arg1= "'" +rules_df_col+"'," + result_format
                    function_name = function_name.replace("arg1",arg1)
    
                print(" Before data_df.{function_name} ")
                function_name = f"""data_df.{function_name}"""
                response = eval(function_name)
                print(function_name)
                if response["success"] == False:
                    print("Inside False condition")
    
                    failed_records_list = response["result"]["unexpected_index_list"]
    
                    failed_df = data_df.iloc[failed_records_list]
                    failed_df['json'] = failed_df.to_json(orient='records')
                    df_count = int(len(data_df.index))
    
                    failed_count = int(response["result"]["unexpected_count"])
    
                    failed_percentage = round(((1-(df_count - failed_count) / df_count)) * 100, 2)
    
                    for i, json_record in failed_df['json'].iloc[:1].items():
                        insert_query = f"""insert into DQ_Violations(Id ,run_id ,rule_id ,Rundatetime ,Comments , Row_scanned ,Failed_count ,Failed_percentage ,Failed_records ,Insert_timestamp) select DQ_VIOLATIONS_SEQ.NEXTVAL,'{runid}','{rule_df_rules}',current_timestamp(),'{rule_df_rule_exp}',{df_count},{failed_count},{failed_percentage},parse_json('{json_record}'),current_timestamp() """
                        df = session.sql(insert_query)
                        df.show()
                        session.sql("commit;")
                    failed_df.drop(['json'], axis=1, inplace=True)
                update_trust_score(session, rules_df_col, rules_df_tbl, failed_percentage)
            else:
                pass
        print("Data Quality Rules check completed with Rule Runid :" + runid)
        return runid
    
    def get_function_rule_map(session) -> dict:
        df = session.sql("select * from GE_FUNCTION_RULE_MAP")
        count = df.count()
        map = {}
        if count == 0:
            return map
        else:
            i = 0
            while count>i:
                rulename = df.select("RULENAME").collect()[i][0]
                ge_functionname = df.select("GE_FUNCTIONNAME").collect()[i][0]
                map[rulename] = ge_functionname
                i+= 1
        return map
    
    
    def get_data_df(session,db_name,schema_name,table_name):
        db = db_name.replace('"', '')
        schema = schema_name.replace('"', '')
        tbl_name = table_name.replace('"', '')
        query = f""" select * from {db}.{schema}.{tbl_name}"""
        data_df = session.sql(query)
        df=data_df.to_pandas()
        data_df = gx.from_pandas(df)
        return data_df,tbl_name
    
    def get_rules_df(session,tbl_name:str):
        rules_query = f""" select * from ( select a.column_name,a.data_type,dr.Rulename,dr.rule_expression,a.table_name, row_number() over (partition by a.column_name,dr.Rulename order by a.column_name ) as row_num from DQ_RULES dr, attributes a where dr.column_id = a.ID and lower(a.Table_name) = '{tbl_name}' and is_active = True and current_timestamp() between start_date and end_date) where row_num = 1 order by column_name"""
        rules_df = session.sql(rules_query)
        print("Rules Found : " + str(rules_df.count()))
        return rules_df

    
    data_df,tbl_name = get_data_df(session,db_name,schema_name,table_name)
    rules_df = get_rules_df(session,tbl_name)
    runid = great_expectation_dq_check(session,data_df,rules_df)
    print("Check dq_violations Table for Failed Records")
    #except Exception as e:
    #print("Exception occured " + str(e))

    # Return value will appear in the Results tab.
    return runid
    
$$;

-- Process Collect metadata


CREATE OR REPLACE PROCEDURE DATA_GOV_METADATA_APP.collect_processmetadata(db_name VARCHAR, schema_name VARCHAR, table_name VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$

from snowflake.snowpark import Session
import random


def process_metadata(session,db, schema, tbl_name:str):
    query_history = f""" select session_id, end_time, TOTAL_ELAPSED_TIME from snowflake.account_usage.query_history where 
    lower(query_text) like '%insert into {tbl_name} %' and lower(SCHEMA_NAME) = coalesce('{schema}',SCHEMA_NAME) 
    and lower(DATABASE_NAME) = coalesce('{db}',DATABASE_NAME)  """

    query_history_df = session.sql(query_history)

    load_history = f""" select ROW_COUNT, FILE_NAME, ROW_PARSED from snowflake.account_usage.load_history where
    lower(SCHEMA_NAME) = coalesce('{schema}', SCHEMA_NAME) and lower(TABLE_NAME) = coalesce('{tbl_name}', TABLE_NAME) """

    load_history_df = session.sql(load_history)


    if query_history_df.count() > 0 and load_history_df.count() > 0:
        query_history_df.show()
        SESSION_ID = query_history_df.select("SESSION_ID").collect()[0][0]
        END_DATE = query_history_df.select("END_TIME").collect()[0][0]
        TOTAL_ELAPSED_TIME = query_history_df.select("TOTAL_ELAPSED_TIME").collect()[0][0]
        ROW_COUNT = load_history_df.select("ROW_COUNT").collect()[0][0]
        FILE_NAME = load_history_df.select("FILE_NAME").collect()[0][0]
        ROW_PARSED = load_history_df.select("ROW_PARSED").collect()[0][0]

        insert_query = f""" INSERT INTO PROCESS_METADATA(PIPELINE_ID,SESSIONID,RUNDATE,SOURCE_TABLE,TARGET_TABLE,RECORDSPROCESSED ,FILESPROCESSED,RECORDSLOADED ,TIMETAKEN)
        values(PROCESS_METADATA_SEQ.NEXTVAL,{SESSION_ID},current_timestamp(),'{tbl_name}','{tbl_name}',{ROW_COUNT},'{FILE_NAME}',{ROW_PARSED},{TOTAL_ELAPSED_TIME}) """
        print(insert_query)
        insert_query_df = session.sql(insert_query)
        insert_query_df.show()


def main(session,db_name,schema_name,table_name):
    try:
        metadata_df = process_metadata(session,db_name.lower(),schema_name.lower(),table_name.lower())
        return "Success"
    except Exception as e:
        return "Exception occured" + str(e)
$$;

-----------------
