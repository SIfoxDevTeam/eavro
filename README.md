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
Encode value of union type require explicit type specification when encoding:

```erlang
1> rr(eavro).
[avro_array,avro_enum,avro_fixed,avro_map,avro_record]
2> eavro:encode([int, string], {int, 1}).
<<0,2>>
3> eavro:encode([int, string], {string, <<"blah">>}).
<<2,8,98,108,97,104>>
4> eavro:encode(#avro_array{ items = [int, string] }, [{int, 1}, {string, <<"blah">>}]).
<<4,0,2,2,8,98,108,97,104,0>>
5> eavro:decode(#avro_array{ items = [int, string] }, <<4,0,2,2,8,98,108,97,104,0>>).
{[[1,<<"blah">>]],<<>>}
6> RecType = #avro_record{ name = some_struct, fields = [{field1, int}] }.
#avro_record{name = some_struct,fields = [{field1,int}]}
7> eavro:encode(#avro_array{ items = [int, string, RecType] }, [{int, 1}, {string, <<"blah">>}, {RecType, [37337] }]).
<<6,0,2,2,8,98,108,97,104,4,178,199,4,0>>
8> eavro:decode(#avro_array{ items = [int, string, RecType] }, <<6,0,2,2,8,98,108,97,104,4,178,199,4,0>>).            
{[[1,<<"blah">>,[37337]]],<<>>}
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


Read data from Avro binary file in an OCF format using eavro_ocf_zcodec:

```erlang
1> rr(eavro).
[avro_array,avro_enum,avro_fixed,avro_map,avro_record]
2> eavro_ocf_zcodec:read_ocf_with(
2>              "test/data/transformers-deflated.avro",
2>             fun(Schema, ZInstances) ->
2>                     ZInstances
2>             end).
[[<<"0000">>,<<"Optimus">>,<<"Prime">>,1000,true,'Earth',
  [[{<<"weapon">>,[<<"SuperBlaster">>,33]}]],
  [[<<"0001">>,<<"0002">>]],
  234]|
 #Fun<eavro_zcodec.3.24606246>]
3> eavro_ocf_zcodec:read_ocf_with(
3>              "test/data/transformers-deflated.avro",
3>             fun(Schema, ZInstances) ->
3>                     zlists:count(ZInstances)
3>             end).
12
4> eavro_ocf_zcodec:read_ocf_with(
4>              "test/data/transformers-deflated.avro",
4>             fun(Schema, ZInstances) -> 
4>                 zlists:expand(5, 
4>		       zlists:map(fun(Inst) -> hd(Inst) end, ZInstances)) 
4>	       end).
[<<"0000">>,<<"0001">>,<<"0002">>,<<"0003">>,<<"0004">>|
 #Fun<zlists.6.49696493>]
```
The function 'eavro_ocf_zcodec:read_ocf_with' gives a way for memory effecient way to read huge Avro OCF files. Currently only 'deflate' compression codec supported (snappy TBD).

Writing OCFs:

```erlang
1> rr(eavro).
[avro_array,avro_enum,avro_fixed,avro_map,avro_record]
2> Schema = eavro:read_schema("test/data/twitter.avsc").
#avro_record{name = twitter_schema,
             fields = [{<<"username">>,string},
                       {<<"tweet">>,string},
                       {<<"timestamp">>,long}]}
3> eavro:write_ocf("data.avro", Schema, [ [<<"Optimus">>, <<"Prime">>, 134234132], [<<"Nexus">>, <<"Prime">>, 3462547657] ]).
ok
4> eavro:read_ocf_with("data.avro", fun(_Schema, ZInstances) -> zlists:expand(ZInstances) end ).
[[<<"Optimus">>,<<"Prime">>,134234132],
 [<<"Nexus">>,<<"Prime">>,3462547657]]
```

Making an Avro RPC calls:

```erlang
1> {ok, P} = eavro_rpc_fsm:start_link("localhost", 41414, "flume.avpr").
{ok,<0.35.0>}
2> eavro_rpc_fsm:call(P, append, _Args = [ _Rec = [ [], <<"HELLO">> ] ]).
{ok,'OK'}
```

To make an Avro RPC calls you need an Avro protocol file in a JSON format 
(usually *.avpr file), if you have an only Avro IDL file (usually *.avdl file), 
for now you are addressed to the avro tool to make `.avdl -> .avpr` conversion:

```bash
$ mkdir avro_tools
$ (cd avro_tools && wget http://apache-mirror.rbc.ru/pub/apache/avro/avro-1.7.7/java/avro-tools-1.7.7.jar)
$ java -jar avro_tools/avro-tools-1.7.7.jar idl test/data/flume.avdl | python -mjson.tool > flume.avpr
```

ToDo
----

 * Add specs, tests and documentation
 * Add data writer/reader functions
   * Write OCF files
   * Support codecs (snappy) when reading and writing data from OCF

License
-------

All parts of this software are distributed under the Apache License, Version 2.0 terms.
