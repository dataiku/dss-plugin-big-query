# file to retrieve the input and output datasets and parse the UI parameters (recipe_config) into a class or dict

import dataiku
from dataiku.customrecipe import (
    get_input_names_for_role,
    get_output_names_for_role,
)

def get_input_output():
    # retrieve the input and output dataset of the recipe
    input_dataset_name = get_input_names_for_role("input_dataset")[0]
    input_dataset = dataiku.Dataset(input_dataset_name)
    output_dataset_name = get_output_names_for_role("output_dataset")[0]
    output_dataset = dataiku.Dataset(output_dataset_name)
    return input_dataset, output_dataset


def parse_recipe_config(recipe_config):
    # function to parse the UI parameters of recipe_config and put them into a params dict (or better we could do a class)
    # can use this to validate the parameters and raise the appropriate errors to the UI
    params = {}
    params["fields_to_flatten"] = [field for field in recipe_config.get("fieldsToFlatten") if 'path' in field]
    return params
