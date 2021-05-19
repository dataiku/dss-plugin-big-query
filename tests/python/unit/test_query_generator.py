import os
import sys

import pytest

plugin_root = os.path.dirname(os.path.dirname((os.path.dirname(os.path.dirname(os.path.realpath(__file__))))))
sys.path.append(os.path.join(plugin_root, 'python-lib'))

from query_generator import generate_query, compute_unnest_commands, get_unnest_command, get_technical_column_name, get_select_command

class TestQueryGenerator:
    def test_generate_query_simple(self):
        class TestDataset():
            def get_config(self):
                return {
                    "params":{
                        "catalog": "mycatalog",
                        "schema": "myschema",
                        "table": "mytable"
                    }
                }
        dataset = TestDataset()
        # Simple case
        params = {"fields_to_unnest": [{"path": "test"}]}
        print(generate_query(params, dataset))
        assert generate_query(params, dataset) == "SELECT\ntest\nFROM `mycatalog`.`myschema`.`mytable`\n\n"
        params = {"fields_to_unnest": [{"path": "test", "output": "value"}]}
        print(generate_query(params, dataset))
        assert generate_query(params, dataset) == "SELECT\ntest AS value\nFROM `mycatalog`.`myschema`.`mytable`\n\n"


    def test_generate_query_complex(self):
        class TestDataset():
            def get_config(self):
                return {
                    "params":{
                        "catalog": "mycatalog",
                        "schema": "myschema",
                        "table": "mytable"
                    }
                }
        dataset = TestDataset()
        # Simple case
        params = {"fields_to_unnest": [{"path": "testValue"},
                                       {"path": "testValue.hiearchical"},
                                       {"path": "test[].arrayElement[]"},
                                       {"path": "test[].subElement[].subsubElement[].finalElement"},
                                       {"path": "test[].subElement[].subsubElement2[].finalElement"}
                                       ]}
        print(generate_query(params, dataset))
        assert generate_query(params, dataset) == "SELECT\n" \
                                                  "testValue,\n" \
                                                  "testValue.hiearchical,\n" \
                                                  "dku_test__arrayElement_ AS test_arrayElement_,\n" \
                                                  "dku_test__subElement__subsubElement_.finalElement,\n" \
                                                  "dku_test__subElement__subsubElement2_.finalElement\n" \
                                                  "FROM `mycatalog`.`myschema`.`mytable`\n" \
                                                  "LEFT JOIN UNNEST(test) AS dku_test_\n" \
                                                  "LEFT JOIN UNNEST(dku_test_.arrayElement) AS dku_test__arrayElement_\n" \
                                                  "LEFT JOIN UNNEST(dku_test_.subElement) AS dku_test__subElement_\n" \
                                                  "LEFT JOIN UNNEST(dku_test__subElement_.subsubElement) AS dku_test__subElement__subsubElement_\n" \
                                                  "LEFT JOIN UNNEST(dku_test__subElement_.subsubElement2) AS dku_test__subElement__subsubElement2_\n"

    def test_generate_query_complex_with_value(self):
        class TestDataset():
            def get_config(self):
                return {
                    "params":{
                        "catalog": "mycatalog",
                        "schema": "myschema",
                        "table": "mytable"
                    }
                }
        dataset = TestDataset()
        # Simple case
        params = {"fields_to_unnest": [{"path": "testValue", "output": "value1"},
                                       {"path": "testValue.hiearchical", "output": "value2"},
                                       {"path": "test[].arrayElement[]", "output": "value3"},
                                       {"path": "test[].subElement[].subsubElement[].finalElement", "output": "value4"},
                                       {"path": "test[].subElement[].subsubElement2[].finalElement", "output": "value5"}
                                       ]}
        print(generate_query(params, dataset))
        assert generate_query(params, dataset) == "SELECT\n" \
                                                  "testValue AS value1,\n" \
                                                  "testValue.hiearchical AS value2,\n" \
                                                  "dku_test__arrayElement_ AS value3,\n" \
                                                  "dku_test__subElement__subsubElement_.finalElement AS value4,\n" \
                                                  "dku_test__subElement__subsubElement2_.finalElement AS value5\n" \
                                                  "FROM `mycatalog`.`myschema`.`mytable`\n" \
                                                  "LEFT JOIN UNNEST(test) AS dku_test_\n" \
                                                  "LEFT JOIN UNNEST(dku_test_.arrayElement) AS dku_test__arrayElement_\n" \
                                                  "LEFT JOIN UNNEST(dku_test_.subElement) AS dku_test__subElement_\n" \
                                                  "LEFT JOIN UNNEST(dku_test__subElement_.subsubElement) AS dku_test__subElement__subsubElement_\n" \
                                                  "LEFT JOIN UNNEST(dku_test__subElement_.subsubElement2) AS dku_test__subElement__subsubElement2_\n"

    def test_get_select_command(self):
        # Simple case
        assert get_select_command("test") == "test"
        assert get_select_command("test", "value") == "test AS value"
        assert get_select_command("test.hierarchical.on.multiple.level") == "test.hierarchical.on.multiple.level"
        assert get_select_command("test.hierarchical.on.multiple.level", "value") == "test.hierarchical.on.multiple.level AS value"

        # First level array
        assert get_select_command("test.hierarchical.on.multiple.level[]") == "dku_test_hierarchical_on_multiple_level_ AS test_hierarchical_on_multiple_level_"
        assert get_select_command("test.hierarchical.on.multiple.level[]", "value") == "dku_test_hierarchical_on_multiple_level_ AS value"


        assert get_select_command("test.hierarchical.on.multiple.level[].final.element") == \
               "dku_test_hierarchical_on_multiple_level_.final.element"
        assert get_select_command("test.hierarchical.on.multiple.level[].final.element", "value") == \
               "dku_test_hierarchical_on_multiple_level_.final.element AS value"

        assert get_select_command("test.hierarchical.on.multiple.level[].sub.element[]") == \
               "dku_test_hierarchical_on_multiple_level__sub_element_ AS test_hierarchical_on_multiple_level_sub_element_"
        assert get_select_command("test.hierarchical.on.multiple.level[].sub.element[]", "value") == \
               "dku_test_hierarchical_on_multiple_level__sub_element_ AS value"


        assert get_select_command("test.hierarchical.on.multiple.level[].sub.element[].final.element") == \
               "dku_test_hierarchical_on_multiple_level__sub_element_.final.element"
        assert get_select_command("test.hierarchical.on.multiple.level[].sub.element[].final.element", "value") == \
               "dku_test_hierarchical_on_multiple_level__sub_element_.final.element AS value"


    def test_compute_unnest_commands(self):
        # No unnest
        assert compute_unnest_commands([{"path": "test"}]) == []

        # Simple case
        assert compute_unnest_commands([{"path": "test[]"}]) == ["LEFT JOIN UNNEST(test) AS dku_test_"]
        # we want the unnest part, so the final element is discarded
        assert compute_unnest_commands([{"path": "test[].finalElement"}]) == ["LEFT JOIN UNNEST(test) AS dku_test_"]

        # Two levels unnesting
        assert compute_unnest_commands([{"path": "test[].subElement[]"}]) == \
               ["LEFT JOIN UNNEST(test) AS dku_test_",
                "LEFT JOIN UNNEST(dku_test_.subElement) AS dku_test__subElement_"]
        # we want the unnest part, so the final element is discarded
        assert compute_unnest_commands([{"path": "test[].subElement[].finalElement"}]) == \
               ["LEFT JOIN UNNEST(test) AS dku_test_",
                "LEFT JOIN UNNEST(dku_test_.subElement) AS dku_test__subElement_"]

        # Two levels unnesting, with common path (we don't want to unnest dku_test_ multiple time)
        assert compute_unnest_commands([{"path": "test[].subElement[]"}, {"path": "test[].subElement2[]"}]) == \
               ["LEFT JOIN UNNEST(test) AS dku_test_",
                "LEFT JOIN UNNEST(dku_test_.subElement) AS dku_test__subElement_",
                "LEFT JOIN UNNEST(dku_test_.subElement2) AS dku_test__subElement2_"]
        # we want the unnest part, so the final element is discarded
        assert compute_unnest_commands([{"path": "test[].subElement[].finalElement"}, {"path": "test[].subElement2[].finalElement"}]) == \
               ["LEFT JOIN UNNEST(test) AS dku_test_",
                "LEFT JOIN UNNEST(dku_test_.subElement) AS dku_test__subElement_",
                "LEFT JOIN UNNEST(dku_test_.subElement2) AS dku_test__subElement2_"]

    def test_get_unnest_command(self):
        assert get_unnest_command("test.path", "test_path") == "LEFT JOIN UNNEST(test.path) AS test_path"

    def test_get_technical_column_name(self):

        assert get_technical_column_name(["a"]) == "a_"
        assert get_technical_column_name(["b.c"]) == "b_c_"
        assert get_technical_column_name(["a", "b.c", "d.e.f"]) == "a_b_c_d_e_f_"
