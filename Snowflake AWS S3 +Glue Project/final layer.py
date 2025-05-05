###############################################################################
# Step 1 - To start, we must first import our Snowpark Package
###############################################################################
# Note: You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, sum, year, quarter, to_date, concat, lit

###############################################################################
# Step 2 - Setup the current database
###############################################################################

def main(session: snowpark.Session):

    # Use SQL to set the current database and schema
    session.sql('USE SCHEMA SNOWPARK_DB.TRANSFORMED').collect()

###############################################################################
# Curation 1 - Create a Global Sales table only with the delivery status as Delivered
###############################################################################

    # Global Sales order delivered
    df_global_sales_order_delivered = session.table('SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER') \
        .filter(col("SHIPPING_STATUS") == 'Delivered')

    # Load the data into the table
    df_global_sales_order_delivered.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.CURATED.GLOBAL_SALES_ORDER_DELIVERED")

###############################################################################
# Curation 2 - Create Global Sales tables by aggregating the sales based on mobile brand
###############################################################################

    # Global Sales order brand
    df_global_sales_order_brand = session.table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER") \
        .groupBy(col("MOBILE_BRAND"), col("MOBILE_MODEL")) \
        .agg(sum(col("TOTAL_PRICE")).alias("TOTAL_SALES_AMOUNT"))

    # Load the data into the table
    df_global_sales_order_brand.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.CURATED.GLOBAL_SALES_ORDER_BRAND")
###############################################################################
# Curation 3 - Create a Global Sales table by aggregating the sales based on country
###############################################################################

    # Global Sales order country
    df_global_sales_order_country = session.table("SNOWPARK_DB.TRANSFORMED.GLOBAL_SALES_ORDER") \
        .groupBy(
            col("COUNTRY"),
            year(to_date(col("ORDER_DATE"))).alias("YEAR"),
            concat(lit("Q"), quarter(to_date(col("ORDER_DATE")))).alias("QUARTER")
        ) \
        .agg(
            sum(col("QUANTITY")).alias("TOTAL_SALES_VOLUME"),
            sum(col("TOTAL_PRICE")).alias("TOTAL_SALES_AMOUNT")
        )
    
    # Load the data into the table
    df_global_sales_order_country.write.mode("overwrite") \
        .save_as_table("SNOWPARK_DB.CURATED.GLOBAL_SALES_ORDER_COUNTRY")
    
    return "Code to load the curated schema tables ran successfully"

