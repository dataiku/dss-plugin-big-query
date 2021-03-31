import dataiku


def do(payload, config, plugin_config, inputs):
    mydataset = dataiku.Dataset("nestedinput")
    schema = mydataset.read_schema()
    schema_columns = [col for col in schema]

    response = {"inputSchema": [
        ["Last layer", "last"],
        ["All layers", "all"],
        ["N last layers", "n_last"]
    ]}

    return response

def yolo(columns, prefix=""):
    output = []
    for col in columns:
        if col["type"] == "array":
            if prefix == "":
                output += [col["name"]]
            else:
                # TODO explain
                output += [prefix + "." + col["name"]]
            output += ["array"]
        elif col["type"] == "object":
            output += ["object"]
        else:
            if prefix == "":
                output += [col["name"]]
            else:
                # TODO explain
                output += [prefix + "." + col["name"]]
    return output
