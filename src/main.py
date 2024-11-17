# import json
# import jsonschema
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    ArrayType,
    DoubleType,
)
from pyspark.sql import SparkSession

exploded_df = json_lines_df.selectExpr("*", "[*] as exploded_array" if 
json_lines_df.select(F.col("*").isNotNull().alias("check")).filter(F.col("cjsonlines_df.select(F.col("*").isNotNull().alias("check")).filter(F.col("check") == False).count() > 0 else "cast (exploded_array[0] as string) as 
exploded_array")

exploded_array_columns = [F.from_json(F.col("exploded_array"), 
array_element_schema).alias(f"array_{i}") for i in 
range(json_lines_df.select(F.size("exploded_array")).collect()[0][0])]

expanded_df = exploded_df.selectExpr(*[c.path[0] for c in 
exploded_array_columns], *exploded_array_columns)



def json_schema_to_pyspark_schema(json_schema):

    def _parse_json_schema(schema):
        if type(schema["type"]) is list:
            if "array" in schema["type"]:
                element_type = _parse_json_schema(schema["items"])
                return ArrayType(element_type, any("null" in x for x in schema["type"]))
            elif "string" in schema["type"]:
                return StringType()
            elif "integer" in schema["type"]:
                return IntegerType()
            elif "boolean" in schema["type"]:
                return BooleanType()
            elif "number" in schema["type"]:
                return DoubleType()
            else:
                raise ValueError(f"Unsupported JSON Schema type: {schema['type']}")
        else:
            if schema["type"] == "object":
                fields = []
                for field_name, field_schema in schema["properties"].items():
                    field_type = _parse_json_schema(field_schema)
                    fields.append(
                        StructField(
                            field_name,
                            field_type,
                            any("null" in x for x in field_schema["type"]),
                        )
                    )
                return StructType(fields)
            elif schema["type"] == "array":
                element_type = _parse_json_schema(schema["items"])
                return ArrayType(element_type)
            elif schema["type"] == "string":
                return StringType()
            elif schema["type"] == "integer":
                return IntegerType()
            elif schema["type"] == "boolean":
                return BooleanType()
            elif schema["type"] == "number":
                return DoubleType()
            else:
                raise ValueError(f"Unsupported JSON Schema type: {schema['type']}")

    # jsonschema.Draft201909Validator.check_schema(json_schema)
    return _parse_json_schema(json_schema)


def main():
    # json_schema = {
    #     "type": "object",
    #     "properties": {
    #         "name": {"type": "string"},
    #         "age": {"type": "integer"},
    #         "is_active": {"type": "boolean"},
    #         "scores": {"type": "array", "items": {"type": "number"}},
    #     },
    # }

    json_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://example.com/product.schema.json",
        "version": "v1",
        "title": "Attendee Schema",
        "description": "Attendee JSON Schema",
        "type": "object",
        "properties": {
            "syncGuid": {
                "description": "unique Sync GUID of the attendee",
                "type": "string",
                "dataClassification": [],
            },
            "firstName": {"type": ["string", "null"], "dataClassification": ["PII"]},
            "lastName": {"type": "string", "dataClassification": ["PII"]},
            "mi": {"type": ["string", "null"], "dataClassification": []},
            "suffix": {"type": ["string", "null"], "dataClassification": []},
            "title": {"type": ["string", "null"], "dataClassification": []},
            "attendeeType": {"type": "string", "dataClassification": []},
            "company": {"type": ["string", "null"], "dataClassification": []},
            "externalId": {"type": ["string", "null"], "dataClassification": []},
            "isDeleted": {"type": ["string", "null"], "dataClassification": []},
            "currency": {
                "description": "3 letter alpha code",
                "type": "string",
                "dataClassification": [],
            },
            "customFields": {
                "description": "list of custom fields. The value is long code if the field is a list field.",
                "type": ["array", "null"],
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "dataType": {
                            "type": "string",
                            "enum": [
                                "VARCHAR",
                                "LIST",
                                "MLIST",
                                "BOOLEANCHAR",
                                "MONEY",
                                "INTEGER",
                                "NUMERIC",
                                "CHAR",
                                "TIMESTAMP",
                            ],
                        },
                        "value": {"type": "string"},
                    },
                    "required": ["id", "dataType", "value"],
                    "dataClassification": [],
                },
            },
            "currentVersionNumber": {
                "description": "current version number of the attendee",
                "type": "string",
                "dataClassification": [],
            },
            "companyId": {
                "description": "company ID of the entity",
                "format": "uuid",
                "type": "string",
                "dataClassification": [],
            },
            "lastModifiedTimestamp": {
                "description": "UTC timestamp of modification",
                "type": "string",
                "format": "date-time",
                "dataClassification": [],
            },
        },
        "required": [
            "syncGuid",
            "lastName",
            "currency",
            "currentVersionNumber",
            "companyId",
            "lastModifiedTimestamp",
        ],
    }

    # print(jsonschema.Draft201909Validator.check_schema(json_schema))

    pyspark_schema = json_schema_to_pyspark_schema(json_schema)
    print(pyspark_schema)

    # Create a spark session
    spark = SparkSession.builder.appName("Empty_Dataframe").getOrCreate()

    # Create an empty RDD
    emp_RDD = spark.sparkContext.emptyRDD()

    data = spark.createDataFrame(data=emp_RDD, schema=pyspark_schema)

    print(data.schema.json())

    new_schema = {
        "fields": [
            {"metadata": {}, "name": "syncGuid", "nullable": false, "type": "string"},
            {"metadata": {}, "name": "firstName", "nullable": true, "type": "string"},
            {"metadata": {}, "name": "lastName", "nullable": false, "type": "string"},
            {"metadata": {}, "name": "mi", "nullable": true, "type": "string"},
            {"metadata": {}, "name": "suffix", "nullable": true, "type": "string"},
            {"metadata": {}, "name": "title", "nullable": true, "type": "string"},
            {
                "metadata": {},
                "name": "attendeeType",
                "nullable": false,
                "type": "string",
            },
            {"metadata": {}, "name": "company", "nullable": true, "type": "string"},
            {"metadata": {}, "name": "externalId", "nullable": true, "type": "string"},
            {"metadata": {}, "name": "isDeleted", "nullable": true, "type": "string"},
            {"metadata": {}, "name": "currency", "nullable": false, "type": "string"},
            {
                "metadata": {},
                "name": "customFields",
                "nullable": true,
                "type": {
                    "containsNull": true,
                    "elementType": {
                        "fields": [
                            {
                                "metadata": {},
                                "name": "id",
                                "nullable": false,
                                "type": "string",
                            },
                            {
                                "metadata": {},
                                "name": "dataType",
                                "nullable": false,
                                "type": "string",
                            },
                            {
                                "metadata": {},
                                "name": "value",
                                "nullable": false,
                                "type": "string",
                            },
                        ],
                        "type": "struct",
                    },
                    "type": "array",
                },
            },
            {
                "metadata": {},
                "name": "currentVersionNumber",
                "nullable": false,
                "type": "string",
            },
            {"metadata": {}, "name": "companyId", "nullable": false, "type": "string"},
            {
                "metadata": {},
                "name": "lastModifiedTimestamp",
                "nullable": false,
                "type": "string",
            },
        ],
        "type": "struct",
    }


if __name__ == "__main__":
    main()
