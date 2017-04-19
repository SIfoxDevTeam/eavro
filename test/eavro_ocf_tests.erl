-module(eavro_ocf_tests).

-include("eavro.hrl").

-include_lib("eunit/include/eunit.hrl").

parse_twitter_ocf_test() ->
    Data = eavro:read_ocf("test/data/twitter.avro"),
    ?assertMatch(
       {_Schema = #avro_record{},
	_Blocks =
	   [_Block =
		[ _Rec1 = [<<"miguno">>,<<"Rock: Nerf paper, scissors is fine.">>,
			   1366150681],
		  _Rec2 = [<<"BlizzardCS">>,<<"Works as intended.  Terran is IMBA.">>,
			   1366154481]
		]
	   ]}, Data).

parse_twitter_ocf_with_hook_test() ->
    {_Schema = #avro_record{},
     Data } = eavro:read_ocf("test/data/twitter.avro", fun simple_hook/2),
    ?assertMatch(
	[{"miguno","Rock: Nerf paper, scissors is fine.",1366150681},
	 {"BlizzardCS","Works as intended.  Terran is IMBA.",1366154481} ],
       lists:flatten(Data)).

parse_transformers_ocf_test() ->
    Data = eavro:read_ocf("test/data/transformers.avro"),
    ?assertMatch(
       {_Schema = #avro_record{},
	_Blocks =
	   [_Block =
		[ _Rec1 = [<<"0000">>, <<"Optimus">>,<<"Prime">> |_] | _ ]
	   ]}, Data).

parse_transformers_ocf_deflated_test() ->
    Data = eavro_ocf_zcodec:read_ocf_with(
	     "test/data/transformers-deflated.avro",
	    fun(Schema, ZInstances) ->
		    { Schema, zlists:expand(ZInstances) }
	    end),
    ?assertMatch(
       {_Schema = #avro_record{},
	_Instances =
	   [
	    _Instance = [<<"0000">>, <<"Optimus">>,<<"Prime">> |_] | _
	   ]}, Data).

ocf_read_write_read_test_() ->
    Test =
	fun(F1, F2, Opts) ->
		file:delete(F2),
		?assert(not filelib:is_regular(F2)),
		ok = eavro_ocf_zcodec:read_ocf_with(
		  F1,
		  fun(Schema, ZInstances) ->
			  eavro_ocf_zcodec:write_ocf_file(
			    F2,
			    Schema, ZInstances, Opts)
		  end,
		  fun eavro_util:std_hook/2 ),
		Cnt = eavro_ocf_zcodec:read_ocf_with(
		  F2,
		  fun(_Schema, ZInstances) ->
			  zlists:count(ZInstances)
		  end),
		?assert( Cnt > 0 )
	end,
    {inorder,
     [{Title,
      fun() -> Test(F1,F2,Opts) end} ||
	 {Title, F1, F2, Opts} <-
	     [{"Read plain, write plain, read result.",
	       "test/data/transformers.avro",
	       "test/transformers.avro",
	       []},
	      {"Read plain, write plain multiblocked, read multiblocked result.",
	       "test/data/transformers.avro",
	       "test/transformers-mult.avro",
	       [{block_size, 128}]},
	      {"Read deflated, write plain, read result.",
	       "test/data/transformers-deflated.avro",
	       "test/transformers-infl.avro",
	       []},
	      {"Read deflated, write deflated, read deflated result.",
	       "test/data/transformers-deflated.avro",
	       "test/transformers-deflated.avro",
	       [{codec, deflated}]},
	      {"Read deflated multiblocked, write deflated, read deflated result.",
	       "test/data/transformers-deflated2.avro",
	       "test/transformers-deflated2.avro",
	       [{codec, deflated}]},
	      {"Read deflated multiblocked, write deflated mult, read deflated result.",
	       "test/data/transformers-deflated2.avro",
	       "test/transformers-deflated2m.avro",
	       [{codec, deflated},{block_size, 128}]}
	     ] ]}.
%%
%% Private functions
%%

simple_hook(#avro_record{name='twitter_schema'},
	    [Name, Tweet, Timestamp]) ->
    {Name, Tweet, Timestamp};
simple_hook(string, V) -> binary_to_list(V);
simple_hook(_, V) -> V.
