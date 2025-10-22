class reuseable:
        
    def dropcolumn(self, df, columns):
        df = df.drop(*columns)
        return df 