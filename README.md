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
 * enum
 * map
 * fixed
 * union

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

Encode record according to its schema:

```erlang

Erlang R16B01 (erts-5.10.2) [source] [64-bit] [smp:2:2] [async-threads:10] [kernel-poll:false]

Eshell V5.10.2  (abort with ^G)
1> Schema = eavro:read_schema("schema.avcs").
{avro_record,<<"User">>,
             [{<<"username">>,string},
              {<<"age">>,int},
              {<<"verified">>,boolean}]}
2> eavro:encode(Schema, [<<"John">>, 23, true]).
<<8,74,111,104,110,46,1>>
```

Read data from Avro binary file in an OCF format:
```erlang
2> rr(eavro).
[avro_enum,avro_fixed,avro_map,avro_record]
3> rp(eavro:read_ocf("test/data/transformers.avro")).
{#avro_record{name = transformer_schema,
              fields = [{<<"fname">>,string},
                        {<<"lname">>,string},
                        {<<"age">>,int},
                        {<<"is_autobot">>,boolean},
                        {<<"location">>,
                         #avro_enum{name = 'Location',
                                    symbols = ['Earth','Moon','March','Venus','Jupiter',
                                               'Mercury','Titan','Io','Europe','Ganimed','Callisto',
                                               'Pluton']}},
                        {<<"equipment">>,
                         #avro_map{values = #avro_record{name = 'Equipment',
                                                         fields = [{<<"name">>,string},{<<"weight">>,int}]}}}]},
 [[[<<"Optimus">>,<<"Prime">>,1000,true,'Earth',
    [[{<<"weapon">>,[<<"SuperBlaster">>,33]}]]],
   [<<"Nexus">>,<<"Prime">>,1001,true,'Moon',
    [[{<<"weapon">>,[<<"PlasmaCanon">>,100]}]]],
   [<<"Zeta">>,<<"Prime">>,2000,true,'March',
    [[{<<"weapon">>,[<<"LazerCanon">>,60]}]]],
   [<<"Rodmus">>,<<"Prime">>,1000,true,'Venus',
    [[{<<"weapon">>,[<<"RocketLauncher">>,200]}]]],
   [<<"Optimus1">>,<<"Prime">>,1000,true,'Jupiter',
    [[{<<"weapon">>,[<<"Blaster">>,33]}]]],
   [<<"Nexus1">>,<<"Prime">>,1001,true,'Mercury',
    [[{<<"weapon">>,[<<"Blaster">>,33]}]]],
   [<<"Zeta1">>,<<"Prime">>,2000,true,'Titan',
    [[{<<"weapon">>,[<<"Blaster">>,33]}]]],
   [<<"Rodmus1">>,<<"Prime">>,1000,true,'Io',
    [[{<<"weapon">>,[<<"Blaster">>,33]}]]],
   [<<"Optimus2">>,<<"Prime">>,1000,true,'Europe',
    [[{<<"weapon">>,[<<"Blaster">>,33]}]]],
   [<<"Nexus2">>,<<"Prime">>,1001,true,'Ganimed',
    [[{<<"weapon">>,[<<"Blaster">>,33]}]]],
   [<<"Zeta3">>,<<"Prime">>,2000,true,'Callisto',
    [[{<<"weapon">>,[<<"NuclearGun">>,70]}]]],
   [<<"Rodmus3">>,<<"Prime">>,1000,true,'Pluton',
    [[{<<"weapon">>,[<<"ElectroHammer">>,180]}]]]]]}
ok
```
Please note how data is returned:
 * the first element of a binary tuple is a schema extracted from OCF header 
 * the second element contains a list of blocks, where each block is a list on schema instances - in our case these are records whose data represented as list of values, that is why we see a deep list structure in a result.

It would be easy to remove such a deep list structure, i.e block lists, but it would lead to use of '++' operator which is not good for performance, hence it was decided to keep block division structure in a result.

The same reason affected to a 'map' type decoding result.

ToDo
----

 * Add specs, tests and documentation
 * Add data writer/reader functions
   * Write OCF files
   * Support codecs (deflat, snappy) when reading and writing data from OCF

License
-------

All parts of this software are distributed under GPLv3 terms.
