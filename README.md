Apache Avro encoder/decoder
===========================

Supported primitives
--------------------

 * null: no value
 * boolean: a binary value
 * int: 32-bit signed integer
 * long: 64-bit signed integer
 * float: single precision (32-bit) IEEE 754 floating-point number
 * double: double precision (64-bit) IEEE 754 floating-point number
 * bytes: sequence of 8-bit unsigned bytes
 * string: unicode character sequence

Supported complex types
-----------------------

 * records

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

Usage
-----

<pre>
Erlang R16B01 (erts-5.10.2) [source] [64-bit] [smp:2:2] [async-threads:10] [kernel-poll:false]

Eshell V5.10.2  (abort with ^G)
1> Schema = eavro:schema("schema.avcs").
{avro_record,<<"User">>,
             [{<<"username">>,string},
              {<<"age">>,int},
              {<<"verified">>,boolean}]}
2> eavro:encode(Schema, [<<"John">>, 23, true]).
<<8,74,111,104,110,46,1>>
</pre>

ToDo
----

 * Add decode functions
 * Add specs, tests and documentation
 * Implement complex types: enums, arrays, maps, unions and fixed.

License
-------

All parts of this software are distributed under GPLv3 terms.
