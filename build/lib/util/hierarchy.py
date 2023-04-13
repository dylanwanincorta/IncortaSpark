from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

class HierarchyBuilder:

    def check_columns(df, cols):
        df_columns = df.columns

        if (cols in df_columns):
            return True
        else:
            return False
        
    def __init__(input_df,key_cols,parent_cols, num_of_lvls, _table_alias="TBL"):
        self.rel_df = input_df
        self.rel_df_columns = input_df.columns
        self.parent_columns = parent_cols
        self.key_columes = key_cols
        self.number_of_levels = num_of_lvls
        self.table_alias = _table_alias

        if not self.check_columns(input_df, key_cols):
            raise ValueError("key columns are not in the dataframe")
        if not self.check_columns(input_df, parent_cols):
            raise ValueError("parent columns are not in the dataframe")

    def check_unique(input_df, key_cols):
        row_count = input_df.count()
        key_count = input_df.select(key_cols).distinct().count()

        if row_count == key_count
            return True
        else:
            return False

    def check_foreign_key(input_df1, key_cols, input_df2, foreign_key_cols):
        df1 = input_df1.select(key_cols)
        df2 = input_df2.select(foreign_key_cols).distinct()

        df_left = df2.exceptAll(df1)
        if df_left.count() > 0:
            return False
        else:
            return True

    def _join_clause(nLevel):

        parent_alias = self.table_alias + str(nLevel-1)
        child_alias = self.table_alias + str(nLevel)

        join_stmt = "\n AND ".join([parent_alias + "." + c + " = " + \
                                    child_alias + "." + self.parent_columns(ind) \ 
                     for ind, c in enumerate(self.key_columes)])
        
        return join_stmt

    def union_all(s, newsql):
        return s + "\nUNION ALL \n" + newsql

    def new_level_join(nLevel, fc):
        return nLevel + 1, \
               fc + "\nINNER JOIN " + self.table_alias + str(nLevel) \
                  + "\n        ON " \
                  + self._join_clause(nLevel)

    def new_select(nLevel):
        ## we assume level 0 is the base like, floors
        ## level 1 is the first and initial value of nLevel is 1
        ## using tree, base is the root level
        select_parent_columns = ",".join([self.table_alias + str(nLevel) + c + " as parent_" + c \
                                 for c in self.parent_columns])
        select_child_columns = ",".join([self.table_alias + "0" + c + " as base_" + c \
                                 for c in self.key_cols])
        
        ## path column can be added here if we need it

        select_distance = str(nLevel) + " as distance"

        return select_parent_columns + ", " + select_distance + ", " + select_child_columns

    def flatteningSQL():
        
        l = 0
        from_sql = self.table_alias + str(l)
        full_sql = ""
        while l <= nums_of_levels:
            select_sql = new_select(l)
            l, from_sql = new_level_join(l, from_sql)
        
            full_sql = union_all(full_sql, new_sql)

        return full_sql

