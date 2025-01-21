field_mappings = {
    "example_table": {
        0: "id",
        1: "foreign_id",
        2: "some_fields",
    },
    "skip_table": {
        0: "id",
        1: "some_fields_0",
    },
}

field_mappings_raw = {
    "example_table": {
        0: "id",
        1: "foreign_id",
        # 2: "some_fields"
        # Field name change
        2: "some_fields_change"
    },
    "skip_table": {
        0: "id",
        1: "some_fields_0",
    },
}

if __name__ == "__main__":
    pass
