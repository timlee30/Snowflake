Use schema snowflake_db.public;
---------------------
CREATE TABLE RAW_SOURCE(JSON_PAYLOAD VARIANT,

PAYLOAD_NUMBER NUMBER(10,0),

INSERT_DTS DATETIME);

---------------------

INSERT INTO 

RAW_SOURCE 

SELECT 

parse_json('{"data": {"empId": "101", 

        "name": "Robert", 

        "age": "34", 

                              "gender": "Male", 

                                                       "location": "Houston", 

        "date": "2023-04-05"

},

"metadata": {"status": "insert",

"lastModified": "2023-06-16T00:00:00.000000Z",

}

}')

,1

,CURRENT_TIMESTAMP();


-----------------------------

INSERT INTO 

RAW_SOURCE 

SELECT 

parse_json('{

"data": {"empId": "102",

"name": "Sam", 

"age": "29", 

"gender": "Male", 

"location": "Dallas", 

"date": "2023-03-21"

},

"metadata": {

"status": "insert",

"lastModified": "2023-06-16T00:00:00.000000Z",

}

}')

,2

    ,CURRENT_TIMESTAMP();

 

INSERT INTO 

RAW_SOURCE 

SELECT 

parse_json('{

"data": {"empId": "103", 

"name": "Smith", 

"age": "25", 

"gender": "Male", 

"location": "Texas", 

"date": "2023-04-10"

},

"metadata": {

"status": "insert",

"lastModified": "2023-06-16T00:00:00.000000Z",

}

}')

,3

    ,CURRENT_TIMESTAMP();

 

INSERT INTO 

RAW_SOURCE 

SELECT 

parse_json('{

"data": {"empId": "104", 

"name": "Dan", 

"age": "31", 

"gender": "Male", 

"location": "Florida", 

"date": "2023-02-07"

},

"metadata": {

"status": "insert",

"lastModified": "2023-06-16T00:00:00.000000Z",

}

}')

,4

    ,CURRENT_TIMESTAMP();

 

INSERT INTO 

RAW_SOURCE 

SELECT 

parse_json('{

"data": {"empId": "105",

"name": "Lily", 

"age": "27", 

"gender": "Female", 

"location": "Cannes", 

"date": "2023-01-30"

},

"metadata": {

"status": "insert",

"lastModified": "2023-06-16T00:00:00.000000Z",

}

}')

,5

    ,CURRENT_TIMESTAMP();

------------------------------------------------------ create a transform/staging table to flatten  data

CREATE TABLE EMPLOYEE_XFM(EMPID INTEGER NOT NULL,

  NAME VARCHAR(50) NOT NULL,

  AGE INTEGER NOT NULL,

  GENDER VARCHAR(10) NOT NULL,

  LOCATION VARCHAR(50) NOT NULL,

  DATE DATE,

  RECORD_TIMESTAMP TIMESTAMP_NTZ(6) NOT NULL,

  SOURCE_SYSTEM VARCHAR(20) NOT NULL COLLATE 'en-ci',

  RECORD_STATUS VARCHAR(50) NOT NULL COLLATE 'en-ci',

  PROCESS_ID VARCHAR(100) NOT NULL COLLATE 'en-ci',

  PROCESS_NAME VARCHAR(200) NOT NULL COLLATE 'en-ci',

  INSERT_DTS TIMESTAMP_NTZ(6) NOT NULL,

  UPDATE_DTS TIMESTAMP_NTZ(6) NOT NULL,

  MD5_HASH VARCHAR(80) NOT NULL COLLATE 'en-ci'

  );
----------------------------
CREATE TABLE EMPLOYEE(EMPLOYEE_ID INTEGER,

  EMPLOYEE_NAME VARCHAR(50),

  EMPLOYEE_AGE INTEGER,

  EMPLOYEE_GENDER VARCHAR(10),

  EMPLOYEE_LOCATION VARCHAR(50),

  DATE DATE,

  RECORD_TIMESTAMP TIMESTAMP_NTZ(6) NOT NULL,

  SOURCE_SYSTEM VARCHAR(20) NOT NULL COLLATE 'en-ci',

  RECORD_STATUS VARCHAR(50) NOT NULL COLLATE 'en-ci',

  PROCESS_ID VARCHAR(100) NOT NULL COLLATE 'en-ci',

  PROCESS_NAME VARCHAR(200) NOT NULL COLLATE 'en-ci',

  INSERT_DTS TIMESTAMP_NTZ(6) NOT NULL,

  UPDATE_DTS TIMESTAMP_NTZ(6) NOT NULL,

  MD5_HASH VARCHAR(80) NOT NULL COLLATE 'en-ci'

  );

  -------------------------------------------
CREATE TABLE JOB_CONTROL (

SESSION_ID INTEGER,

PROCESS_START_DATETIME TIMESTAMP(6),

PROCESS_NAME VARCHAR(100),

PROCESS_END_DATETIME TIMESTAMP(6),

LOW_WATERMARK_DATETIME TIMESTAMP(6),

HIGH_WATERMARK_DATETIME TIMESTAMP(6),

STATUS VARCHAR(100),

FAILURE_REASON VARCHAR(100),

INSERT_DTS TIMESTAMP_NTZ(6)

);

----------------------------------------insert a row into the job_control table to initiate the process of watermarking
INSERT INTO 

    JOB_CONTROL

VALUES(CURRENT_SESSION()

  ,CURRENT_TIMESTAMP()

  ,'Employee Load'

  ,NULL

  ,'1900-01-01 00:00:00.000'

  ,'1900-01-01 00:00:00.000'

  ,NULL

  ,NULL

       ,CURRENT_TIMESTAMP()

)

;

---------------------------------------step 6 :create a stored procedure named for the ETL load. and then ran later 


CREATE OR REPLACE PROCEDURE EMPLOYEE_SRC_TO_TGT_SP(

DB VARCHAR(50),

    PROC_NM VARCHAR(50)

    )

    RETURNS VARCHAR(50)

    LANGUAGE JAVASCRIPT

    EXECUTE AS CALLER

    AS 

    $$

 

    try {

// Update the job control table about the start of the process

     snowflake.execute({sqlText:`CALL ${DB}.PUBLIC.WATERMARK_UPDATE_JOB_CONTROL_SP('${DB}', '${PROC_NM}');`});

    

    //Create statement BEGIN, Begins a transaction in the current session 

     snowflake.execute({sqlText:`BEGIN TRANSACTION;`});

 

     // Store condition result in variable

     var condition_sql_command = `SELECT COUNT(*) FROM ${DB}.PUBLIC.RAW_SOURCE;`;

      

     var condition_sql_statement =  snowflake.createStatement({ sqlText: condition_sql_command });

     var condition_result =  condition_sql_statement.execute();

          

     condition_result.next();

          

     var row_count = condition_result.getColumnValue(1);

     

     // Condition statement to determine if SOURCE table has data

     if( row_count < 1 ) {

          // Terminate transaction if condition equals 0

          snowflake.execute({sqlText:`COMMIT;`});

          return "No data in SOURCE table. Transaction closed.";

     }

     else {

//load data from SOURCE to XFM Table in RAWHIST

snowflake.execute({sqlText:`

INSERT INTO

            ${DB}.PUBLIC.EMPLOYEE_XFM

        (

         EMPID

,NAME

,AGE

,GENDER

,LOCATION

,DATE

,RECORD_TIMESTAMP

         ---- snowflake metadata columns ----

,SOURCE_SYSTEM

,RECORD_STATUS

,PROCESS_ID

,PROCESS_NAME

,INSERT_DTS

,UPDATE_DTS

,MD5_HASH

        )

        SELECT

           JSON_PAYLOAD:data:empId AS EMPID,

           JSON_PAYLOAD:data:name AS NAME,

           JSON_PAYLOAD:data:age AS AGE,

           JSON_PAYLOAD:data:gender AS GENDER,

           JSON_PAYLOAD:data:location AS LOCATION,

           JSON_PAYLOAD:data:date AS DATE,

           JSON_PAYLOAD:metadata:lastModified::timestamp_ntz(6) AS RECORD_TIMESTAMP, --'The date/timestamp of the data from the source.',

           ---- snowflake metadata columns ----

           'Employee' AS SOURCE_SYSTEM,

           JSON_PAYLOAD:metadata:status::string AS RECORD_STATUS,           

           CURRENT_SESSION() AS PROCESS_ID,

           '${PROC_NM}' AS PROCESS_NAME,

           CURRENT_TIMESTAMP(6) AS INSERT_DTS,

           CURRENT_TIMESTAMP(6) AS UPDATE_DTS,

           MD5(

              COALESCE(TO_VARCHAR(TRIM(NAME)), '') ||

              COALESCE(TO_VARCHAR(TRIM(AGE)), '') ||

              COALESCE(TO_VARCHAR(TRIM(GENDER)), '') ||

              COALESCE(TO_VARCHAR(TRIM(LOCATION)), '')

           ) AS MD5_HASH

        FROM 

            ${DB}.PUBLIC.RAW_SOURCE

WHERE 

            EMPID is NOT NULL

        AND

            RECORD_TIMESTAMP > COALESCE((SELECT LOW_WATERMARK_DATETIME FROM ${DB}.PUBLIC.JOB_CONTROL WHERE PROCESS_NAME = '${PROC_NM}' AND STATUS = 'Running' ),'1900-01-01 00:00:00.000')

        QUALIFY ROW_NUMBER() OVER (PARTITION BY EMPID, MD5_HASH ORDER BY RECORD_TIMESTAMP DESC) = 1;

`});

 

//Versioning Notes:

//1. The below SCD Type-1 versioning technique does a INSERT or UPDATE when data comes in for a PK.

 

//UPSERT the records in the TARGET table  - Rawhist Layer

snowflake.execute({sqlText:`

        MERGE INTO

                ${DB}.PUBLIC.EMPLOYEE TGT

            USING

                (SELECT * FROM ${DB}.PUBLIC.EMPLOYEE_XFM WHERE RECORD_TIMESTAMP > COALESCE((SELECT LOW_WATERMARK_DATETIME FROM ${DB}.PUBLIC.JOB_CONTROL WHERE PROCESS_NAME = '${PROC_NM}' AND STATUS = 'Running' ),'1900-01-01 00:00:00.000') QUALIFY ROW_NUMBER() OVER(PARTITION BY EMPID, MD5_HASH ORDER BY RECORD_TIMESTAMP DESC)=1) XFM            ON

                TGT.EMPLOYEE_ID = XFM.EMPID

            WHEN MATCHED THEN

                UPDATE SET 

                    TGT.EMPLOYEE_NAME = XFM.NAME

                    ,TGT.EMPLOYEE_AGE = XFM.AGE

                    ,TGT.EMPLOYEE_GENDER = XFM.GENDER

                    ,TGT.EMPLOYEE_LOCATION = XFM.LOCATION

                    ,TGT.DATE = XFM.DATE

                    ,TGT.RECORD_TIMESTAMP = XFM.RECORD_TIMESTAMP

                    ,TGT.SOURCE_SYSTEM = XFM.SOURCE_SYSTEM

                    ,TGT.RECORD_STATUS = XFM.RECORD_STATUS

                    ,TGT.PROCESS_ID = XFM.PROCESS_ID

                    ,TGT.PROCESS_NAME = XFM.PROCESS_NAME

                    ,TGT.UPDATE_DTS = XFM.UPDATE_DTS

                    ,TGT.MD5_HASH = XFM.MD5_HASH

            WHEN NOT MATCHED THEN

                INSERT (

                    EMPLOYEE_ID

,EMPLOYEE_NAME

,EMPLOYEE_AGE

,EMPLOYEE_GENDER

,EMPLOYEE_LOCATION

,DATE

,RECORD_TIMESTAMP

,SOURCE_SYSTEM

,RECORD_STATUS

,PROCESS_ID

,PROCESS_NAME

,INSERT_DTS

,UPDATE_DTS

,MD5_HASH

                )

                VALUES (

                    XFM.EMPID

,XFM.NAME

,XFM.AGE

,XFM.GENDER

,XFM.LOCATION

,XFM.DATE

,XFM.RECORD_TIMESTAMP

,XFM.SOURCE_SYSTEM

,XFM.RECORD_STATUS

,XFM.PROCESS_ID

,XFM.PROCESS_NAME

,XFM.INSERT_DTS

,XFM.UPDATE_DTS

,XFM.MD5_HASH

                );

            

        `});

 

        //Create statement COMMIT, Commits an open transaction in the current session

 

snowflake.execute({sqlText:`COMMIT;`});

            snowflake.execute({sqlText:`CALL ${DB}.PUBLIC.STATUS_UPDATE_JOB_CONTROL_SP('${DB}', '${PROC_NM}');`});

 

//Statement returned for info and debuging purposes

return "Store Procedure Executed Successfully"; 

}

}

 

    catch (err)  

    {

        result = 'Error: ' + err;

        var e = result.replace(/'/g, ")")

        snowflake.execute({sqlText:`ROLLBACK;`});

        snowflake.execute({sqlText:`CALL ${DB}.PUBLIC.FAILURE_UPDATE_JOB_CONTROL_SP('${DB}', '${PROC_NM}', '${e}');`});

        throw result;

    }

    

    $$;


------------------------- PROCEDURE WATERMARK_UPDATE_JOB_CONTROL_SP
 CREATE OR REPLACE PROCEDURE WATERMARK_UPDATE_JOB_CONTROL_SP(

DB VARCHAR(50),

    PROC_NM VARCHAR(50))

    RETURNS VARCHAR(50)

    LANGUAGE JAVASCRIPT

    EXECUTE AS CALLER

    AS 

    $$

 

    try {

//Create statement BEGIN, Begins a transaction in the current session 

     snowflake.execute({sqlText:`BEGIN TRANSACTION;`});

 

        //load data from SOURCE to XFM Table in RAWHIST

    snowflake.execute({sqlText:`

    UPDATE 

            ${DB}.PUBLIC.JOB_CONTROL

        SET

            PROCESS_START_DATETIME = CURRENT_TIMESTAMP()

        ,LOW_WATERMARK_DATETIME = sq.HIGH_WATERMARK_DATETIME

        ,HIGH_WATERMARK_DATETIME = CURRENT_TIMESTAMP()

        ,STATUS = 'Running'

        FROM(

        SELECT

        HIGH_WATERMARK_DATETIME

        FROM

        ${DB}.PUBLIC.JOB_CONTROL

        WHERE

        PROCESS_NAME = '${PROC_NM}'

        AND

        (STATUS ='Success' OR STATUS IS NULL)         

QUALIFY ROW_NUMBER() OVER(PARTITION BY PROCESS_NAME ORDER BY PROCESS_END_DATETIME DESC) = 1

        )sq

        WHERE INSERT_DTS = (SELECT INSERT_DTS FROM ${DB}.PUBLIC.JOB_CONTROL QUALIFY ROW_NUMBER() OVER(PARTITION BY PROCESS_NAME ORDER BY INSERT_DTS DESC) = 1)

    `});

    

        //Update statement COMMIT, Commits an open transaction in the current session

   

    snowflake.execute({sqlText:`COMMIT;`});

   

    //Statement returned for info and debuging purposes

    return "Store Procedure Executed Successfully"; 

    }

 

    catch (err)  

    {

        result = 'Error: ' + err;

        snowflake.execute({sqlText:`ROLLBACK;`});

        throw result;

    }

    

    $$;

---Step 7:Now create a Snowflake task to run the ETL stored procedure named EMPLOYEE_SRC_TO_TGT_SP in snowflake.


CREATE OR REPLACE TASK SNOWFLAKE_DB.PUBLIC.EMPLOYEE_LOAD_MASTER_SRC_TO_TGT_TSK

    WAREHOUSE = COMPUTE_WH

    COMMENT = 'This task is used to load data to target table'

AS

    CALL SNOWFLAKE_DB.PUBLIC.EMPLOYEE_SRC_TO_TGT_SP('SNOWFLAKE_DB','Employee Load');


--execute
EXECUTE TASK SNOWFLAKE_DB.PUBLIC.EMPLOYEE_LOAD_MASTER_SRC_TO_TGT_TSK;


---Step 8:check the details of the job_control table and employee table

SELECT * FROM SNOWFLAKE_DB.PUBLIC.JOB_CONTROL WHERE PROCESS_NAME = 'Employee Load';
SELECT * FROM SNOWFLAKE_DB.PUBLIC.EMPLOYEE;



--retest first insert into raw_source

INSERT INTO 

RAW_SOURCE 

SELECT 

parse_json('{

"data": {"empId": "102",

"name": "Sam", 

"age": "29", 

"gender": "Male", 

"location": "Texas", 

"date": "2023-05-15"

},

"metadata": {

"status": "update",

"lastModified": "2023-07-23T00:00:00.000000Z",

}

}')

,6

    ,CURRENT_TIMESTAMP();

    

INSERT INTO 

RAW_SOURCE 

SELECT 

parse_json('{

"data": {"empId": "107",

"name": "Billings", 

"age": "28", 

"gender": "Male", 

"location": "Florida", 

"date": "2023-06-12"

},

"metadata": {

"status": "update",

"lastModified": "2023-07-23T00:00:00.000000Z",

}

}')

,7

    ,CURRENT_TIMESTAMP();


----- we should see two new record
SELECT * FROM SNOWFLAKE_DB.PUBLIC.RAW_SOURCE;



----insert a new record in the job_control table 
INSERT INTO 

    JOB_CONTROL

VALUES(CURRENT_SESSION()

  ,CURRENT_TIMESTAMP()

  ,'Employee Load'

  ,NULL

  ,'1900-01-01 00:00:00.000'

  ,'1900-01-01 00:00:00.000'

  ,NULL

  ,NULL

       ,CURRENT_TIMESTAMP()

)

;

select * from JOB_CONTROL


------now resource then redo task 

EXECUTE TASK SNOWFLAKE_DB.PUBLIC.EMPLOYEE_LOAD_MASTER_SRC_TO_TGT_TSK;

select * from JOB_CONTROL



---check back on employee table
SELECT * FROM SNOWFLAKE_DB.PUBLIC.EMPLOYEE




-----
SELECT *
FROM SNOWFLAKE_DB.PUBLIC.JOB_CONTROL
WHERE PROCESS_NAME = 'Employee Load'
ORDER BY INSERT_DTS DESC
LIMIT 5;



