from pyspark.sql.functions import col, lit

assert filter_df.filter(col('city') == lit('Amsterdam')).count() > 0, \
       'There must be customers in Amsterdam'
