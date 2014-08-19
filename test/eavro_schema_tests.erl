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
