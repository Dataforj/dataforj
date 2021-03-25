from pyspark.sql.functions import col, lit

return result_df.filter(col('flag') == lit(True))
