###############################################################################
# Step 1 - To start, we must first import our Snowpark Package
###############################################################################
## Note: You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, split, current_timestamp

###############################################################################
# Step 2 - Setup the current database
###############################################################################

def main(session: snowpark.Session):

    # Use SQL to set the current database and schema
    session.sql('USE SCHEMA SNOWPARK_DB.RAW').collect()

###############################################################################
# Transformation 1 - Order the columns in a sequential order,
# rename few columns, and add country column
###############################################################################

    # India Sales order
    df_india_sales_order = session.sql("""SELECT
        ORDER_ID,
        CUSTOMER_NAME,
        MOBILE_MODEL,
        QUANTITY,
        PRICE_PER_UNIT,
        TOTAL_PRICE,
        PROMOTION_CODE,
        ORDER_AMOUNT,
        GST,
        ORDER_DATE,
        PAYMENT_STATUS,
        SHIPPING_STATUS,
        PAYMENT_METHOD,
        PAYMENT_PROVIDER,
        MOBILE,
        DELIVERY_ADDRESS
        FROM SNOWPARK_DB.RAW.INDIA_SALES_ORDER""")

    # India Sales order Renamed
    df_india_sales_order_renamed = df_india_sales_order.rename("GST", "TAX").rename("MOBILE", "CONTACT_NUMBER")
    
    # Add country field
    df_india_sales_order_country = df_india_sales_order_renamed.withColumn("COUNTRY", lit("INDIA"))
    
    # USA Sales order
    df_usa_sales_order = session.sql("""SELECT
        ORDER_ID,
        CUSTOMER_NAME,
        MOBILE_MODEL,
        QUANTITY,
        PRICE_PER_UNIT,
        TOTAL_PRICE,
        PROMOTION_CODE,
        ORDER_AMOUNT,
        TAX,
        ORDER_DATE,
        PAYMENT_STATUS,
        SHIPPING_STATUS,
        PAYMENT_METHOD,
        PAYMENT_PROVIDER,
        PHONE,
        DELIVERY_ADDRESS
        FROM SNOWPARK_DB.RAW.USA_SALES_ORDER""")
    
    # USA Sales order Renamed
    df_usa_sales_order_renamed = df_usa_sales_order.with_column_renamed("PHONE", "CONTACT_NUMBER")
    
    # Add country field
    df_usa_sales_order_country = df_usa_sales_order_renamed.withColumn("COUNTRY", lit("USA"))


    # France Sales order
    df_france_sales_order = session.sql("""SELECT
        ORDER_ID,
        CUSTOMER_NAME,
        MOBILE_MODEL,
        QUANTITY,
        PRICE_PER_UNIT,
        TOTAL_PRICE,
        PROMOTION_CODE,
        ORDER_AMOUNT,
        TAX,
        ORDER_DATE,
        PAYMENT_STATUS,
        SHIPPING_STATUS,
        PAYMENT_METHOD,
        PAYMENT_PROVIDER,
        PHONE,
        DELIVERY_ADDRESS
        FROM SNOWPARK_DB.RAW.FRANCE_SALES_ORDER""")
    
    # France Sales order Renamed
    df_france_sales_order_renamed = df_france_sales_order.with_column_renamed("PHONE", "CONTACT_NUMBER")
    
    # Add country field
    df_france_sales_order_country = df_france_sales_order_renamed.withColumn("COUNTRY", lit("FRANCE"))

    
    ###############################################################################
    # Transformation 2 - Union the three data sets
    ###############################################################################
    
    # Union India and USA Sales Order Renamed data
    df_india_usa_sales_order = df_india_sales_order_country.union(df_usa_sales_order_country)
    
    # Union India and USA and France Sales Order Renamed data
    df_union_sales_order = df_india_usa_sales_order.union(df_france_sales_order_country)
    
    # return df_union_sales_order
    
    ###############################################################################
    # Transformation 3 - Fill the Missing values with default values
    ###############################################################################
    
    # Fill missing values in the column Promotion Code
    df_union_sales_order_fill = df_union_sales_order.fillna("NA", subset="promotion_code")


    ###############################################################################
    # Transformation 4 - Split one column into multiple columns
    ###############################################################################
    
    # Split the MOBILE_MODEL column into multiple columns
    df_union_sales_order_split = df_union_sales_order_fill \
        .withColumn("MOBILE_BRAND", split(col("MOBILE_MODEL"), lit("/")).getItem(0).cast("string")) \
        .withColumn("MOBILE_VERSION", split(col("MOBILE_MODEL"), lit("/")).getItem(1).cast("string")) \
        .withColumn("MOBILE_COLOR", split(col("MOBILE_MODEL"), lit("/")).getItem(2).cast("string")) \
        .withColumn("MOBILE_RAM", split(col("MOBILE_MODEL"), lit("/")).getItem(3).cast("string")) \
        .withColumn("MOBILE_MEMORY", split(col("MOBILE_MODEL"), lit("/")).getItem(4).cast("string"))



    ###############################################################################
    # Transformation 5 - Rearranging the columns in the dataframe and add INSERT_DTS column
    ###############################################################################
    
    df_union_sales_order_final = df_union_sales_order_split.select(
        col("ORDER_ID"),
        col("CUSTOMER_NAME"),
        col("MOBILE_BRAND"),
        col("MOBILE_VERSION").alias("MOBILE_MODEL"),
        col("MOBILE_COLOR"),
        col("MOBILE_RAM"),
        col("MOBILE_MEMORY"),
        col("PROMOTION_CODE"),
        col("PRICE_PER_UNIT"),
        col("TOTAL_PRICE"),
        col("ORDER_AMOUNT"),
        col("TAX"),
        col("QUANTITY"),  
        col("ORDER_DATE"),
        col("PAYMENT_STATUS"),
        col("SHIPPING_STATUS"),
        col("PAYMENT_METHOD"),
        col("PAYMENT_PROVIDER"),
        col("CONTACT_NUMBER"),
        col("DELIVERY_ADDRESS"),
        col("COUNTRY")
    ).withColumn("INSERT_DTS", current_timestamp())



    ###############################################################################
    # Transformation 6 - Load the above data into the TRANSFORMED schema table
    ###############################################################################
    
    # Load the data into the table
    df_union_sales_order_final.write.mode("overwrite").save_as_table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER")
    
    return "Code to load the transformed table ran successfully"


    
