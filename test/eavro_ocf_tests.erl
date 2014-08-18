-module(eavro_ocf_tests).

-include("eavro.hrl").

-include_lib("eunit/include/eunit.hrl").

parse_twitter_ocf_test() ->
    OcfSchema = eavro:read_schema("../priv/avro_ocf.avsc"),
    {ok, Bin} = file:read_file("../test/data/twitter.avro"),
    {_Header = [_,Meta,_], Bin1} = eavro_codec:decode(OcfSchema,Bin),
    SchemaJson = proplists:get_value(<<"avro.schema">>, lists:flatten(Meta)),
    Schema = eavro:parse_schema(SchemaJson),
    io:format("^^^^^^^^^^ ~n~p~n", [Schema]),
    exit(test_end).
    %% Codec  = proplists:get_value(<<"avro.codec">>, Meta),
    %% io:format("^^^^^^^^^^ ~n~p~n", [Schema]),
    %% io:format("=============== ~n", []),
    %% ?assertMatch( [], decode_all(Schema, Bin1)).

decode_all(Schema, Buff) ->
    case eavro_codec:decode(Schema, Buff) of
	{Obj, <<>>} ->
	    [Obj];
	{Obj, Buff1} ->
	    [Obj | decode_all(Schema, Buff1)]
    end.
