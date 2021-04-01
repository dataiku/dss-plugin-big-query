# file to create the BigQuery query from the parameters


from dataiku.sql import JoinTypes, Expression, Column, Constant, InlineSQL, SelectQuery, Table, Dialects, toSQL, Window
import logging


def generate_query(params, dataset):
    logging.error("params")
    logging.error(params)
    # TODO: currently faking complex input, needs to be real later
    inputs = [params["field_to_keep"]]
    logging.error("-------------")
    query = SelectQuery()
    query.select_from(dataset)

    output_query = "SELECT\n"
    output_query += ",\n".join([get_column_name("", input) for input in inputs])
    output_query += "\n"

    # TODO support if information are present in the connection
    # TODO unwrap if we retrieve a variable instead of a direct string
    dataset_params = dataset.get_config()["params"]
    output_query += "FROM `" + dataset_params["catalog"] + "`.`" + dataset_params["schema"] + "`.`" + dataset_params["table"] + "`\n"

    output_query += "\n".join([get_unnest_command("", input) for input in inputs])
    return output_query


def generate_query_old(params, dataset):
    """
    Deprecated. keeping it so we get an example of the SelectQuery for a few days.
    """
    # example of a function that would generate a query from params (parsed UI parameters) and the input dataset

    logging.error("params")
    logging.error(params)
    # TODO: currently faking complex input, needs to be real later
    inputs = [params["field_to_keep"]]
    logging.error("-------------")
    query = SelectQuery()
    query.select_from(dataset)
    # query.select([Column("id"), Column("metadata.firstname"), Column("number")])
    # query.select([Column("id"), Column("metadata.firstname"), Column("catch.type"), Column("catch"), Column("nb"), Column("animals"), Column("jbet")])

    input_columns = [ Column(get_column_name("", input)) for input in inputs ]
    query.select(input_columns)
    sql_query = toSQL(query, dataset=dataset, dialect=Dialects.BIGQUERY)

    unnest_statement = """
 join UNNEST(catchphrases) AS catch
 join UNNEST(metadata.favorite.numbers) AS nb
 join UNNEST(metadata.favorite.animals) AS animals
 left join UNNEST(objects) AS jbet
    """
    sql_query += unnest_statement

    #def join(self, tableLikeObj, joinType, joinConditions, operatorBetweenConditions='AND', alias=None):
    # query.select(InlineSQL(unnest_statement))
    # query.join(dataset, "LEFT", "UNNEST(metadata.favorite.numbers)", alias="number")

    # convert the SelectQuery object into a query string using the dialect of the dataset (in our case BigQuery)

    return sql_query

def get_column_name(default_name, path):
    """
    Either return the default name or infer the name form the path
    :param default_name: user defined name
    :param path: path to reach the variable, will be used to generated a name if default_name is empty
    :return: a name
    """
    if default_name and default_name.strip():
        return default_name
    else:
        return path.replace(".", "_").replace("[]", "_")

def get_unnest_command(default_name, path):
    """
    Return the unnest SQL command for a given path
    :param default_name: user defined name for the variable (may be null)
    :param path: path to reach the variable, will be used to generated a name if default_name is empty
    :return: the unnest SQL command as a String
    """
    return "LEFT JOIN UNNEST(" + path + ") AS " + get_column_name(path, default_name) + "\n"