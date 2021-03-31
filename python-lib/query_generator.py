# file to create the BigQuery query from the parameters


from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
from dataiku.core.sql import SQLExecutor2


def generate_query(params, dataset):
    # example of a function that would generate a query from params (parsed UI parameters) and the input dataset

    table = SelectQuery()
    table.select_from(dataset, alias="my_table")
    table.select(Column("*"))
    table.where(Column("numbers", table_name="my_table").ge(Constant(10)))
    table.order_by(Column(params.order_by_column, table_name="my_table"))

    # some custom sql can be used like this for example
    unnest_statement = f"""
    UNNEST({params.columns_to_unnest[0]}) AS catch
left join UNNEST({params.columns_to_unnest[0]}) AS nb
    """
    table.select(InlineSQL(unnest_statement), alias="unnested")

    # convert the SelectQuery object into a query string using the dialect of the dataset (in our case BigQuery)
    sql_query = toSQL(table, dataset=dataset)
    return sql_query
