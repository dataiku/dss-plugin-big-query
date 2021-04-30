import dataiku

PLUGIN_ID = 'big-query'

def do(payload, config, plugin_config, inputs):
    dataset = dataiku.Dataset(inputs[0]["fullName"])
    schema = dataset.read_schema()
    schema_columns = [col for col in schema]
    response = {"inputSchema": get_elements(schema_columns), "pluginId": PLUGIN_ID}
    return response

def get_elements(columns, prefix=""):
    output = []
    for col in columns:
        if col["type"] == "array":
            if col["name"] != "":
                output += [[prefix + col["name"] + " (as is)", prefix + col["name"]]]
                output += [[prefix + col["name"] + "[] (unnest)", prefix + col["name"] + "[]"]]
                array_content = col["arrayContent"]
                if array_content["type"] == "object":
                    output += get_elements(array_content["objectFields"], prefix + col["name"] + "[].")
                # Note: you cannot have array of array in bigquery. Lucky us.
        elif col["type"] == "object":
            output += [[prefix + col["name"], prefix + col["name"]]]
            output += get_elements(col["objectFields"], prefix + col["name"] + ".")
        else:
            if col["name"] != "":
                output += [[prefix + col["name"], prefix + col["name"]]]
    return output
