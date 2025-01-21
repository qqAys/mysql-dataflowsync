# MySQL 5.7

# Export as csv file
sql = """select TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION - 1
from information_schema.columns
where TABLE_SCHEMA = 'database_name'
order by TABLE_NAME, ORDINAL_POSITION"""

path = "./Result.csv"

result = {}

with open(path, "r") as f:
    for i in f.readlines():
        i = i.replace("\n", "")
        table, column, position = i.split(",")
        position = int(position)
        try:
            result[table][position] = column
        except KeyError:
            result[table] = {position: column}

print(result)

# ...
# TODO Convert to field_mappings.py
