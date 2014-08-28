-module(eavro_schema_tests).

-include("eavro.hrl").

-include_lib("eunit/include/eunit.hrl").

parse_twitter_schema_test() ->
    Schema = eavro:read_schema("../test/data/twitter.avsc"),
    ?assertMatch(#avro_record{ name = twitter_schema }, Schema),
    Fields = Schema#avro_record.fields,
    ?assertMatch(
       [string, string, long], 
       [ proplists:get_value(K, Fields) || 
	   K <- [ <<"username">>, <<"tweet">>, <<"timestamp">>] ]).

parse_avro_ocf_header_test() ->
    Schema = eavro:read_schema("../priv/avro_ocf.avsc"),
    ?assertMatch(#avro_record{ name = 'org.apache.avro.file.Header' }, Schema),
    Fields = Schema#avro_record.fields,
    ?assertMatch(
       [#avro_fixed{name='Magic',size=4}, 
	#avro_map{values=bytes}, 
	#avro_fixed{name='Sync',size=16}], 
       [ proplists:get_value(K, Fields) || 
	   K <- [ <<"magic">>, <<"meta">>, <<"sync">>] ]).

parse_transformer_schema_test() ->
    Schema = eavro:read_schema("../test/data/transformer.avsc"),
    ?assertMatch(
       #avro_record{ 
	  name = 'transformer_schema',
	  fields = [{<<"id">>, #avro_fixed{} },
		    {<<"fname">>, string },
		    {<<"lname">>, string } | _]}, 
       Schema).

schema_parse_encode_parse_test() ->
    Schema = eavro:read_schema("../test/data/transformer.avsc"),
    SchemaJson = eavro:encode_schema(Schema),
    ?assertMatch(Schema, eavro:parse_schema(SchemaJson)).
