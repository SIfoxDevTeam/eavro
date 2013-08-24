Apache Avro encoder/decoder
===========================

Schema example
--------------

<pre>
{
    "type": "record",
    "name": "User",
    "fields" : [
        {"name": "username", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "verified", "type": "boolean", "default": "false"}
    ]
}
</pre>
