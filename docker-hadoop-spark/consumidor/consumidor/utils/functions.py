class Functions:

    def qualifyTypeColumn(self, df, columns, expressions):
        for column, expression in zip(columns, expressions):
            df = df.withColumn(column, expression)
        return df