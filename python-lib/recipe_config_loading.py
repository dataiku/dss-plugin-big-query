# file to retrieve the input and output datasets and parse the UI parameters (recipe_config) into a class or dict

import dataiku
from dataiku.customrecipe import (
    get_input_names_for_role,
    get_output_names_for_role,
)
import logging

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
    params["fields_to_unnest"] = [field for field in recipe_config.get("fieldsToUnnest") if 'path' in field]
    return params

def get_partitions(output_dataset):
    partitions = []
    if output_dataset.writePartition: # is None is no partitions
        write_partition = output_dataset.writePartition.split("|")
        setting_partitioning = dataiku.api_client().get_default_project().get_dataset(output_dataset.short_name).get_settings().settings["partitioning"]
        dimensions = setting_partitioning.get("dimensions")
        if (len(write_partition) == len(dimensions)):
            for i in range(len(write_partition)):
                partitions.append({"name": dimensions[i].get("name"), "value": write_partition[i]})
            logging.info("Partitions:")
            logging.info(partitions)
        else:
            logging.warn("Dimension not compatible with partition values")
            logging.warn("Continue without partitioning")
    else:
        logging.info("No partitions")
    return partitions