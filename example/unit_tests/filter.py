from pyspark.sql.functions import col, lit

assert filter_df.filter(col('flag') == lit(False)).count() == 0, \
       '0 rows with flag false should be returned'
