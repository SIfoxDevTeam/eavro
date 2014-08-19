-module(eavro_ocf_tests).

-include("eavro.hrl").

-include_lib("eunit/include/eunit.hrl").

parse_twitter_ocf_test() ->
    Data = eavro:read_ocf("../test/data/twitter.avro"),
    ?assertMatch( 
       {Schema = #avro_record{},
	_Blocks = 
	   [_Block = 
		[ _Rec1 = [<<"miguno">>,<<"Rock: Nerf paper, scissors is fine.">>,
			   1366150681],
		  _Rec2 = [<<"BlizzardCS">>,<<"Works as intended.  Terran is IMBA.">>,
			   1366154481]
		]
	   ]}, Data).

parse_twitter_ocf_with_hook_test() ->
    {Schema = #avro_record{}, 
     Data } = eavro:read_ocf("../test/data/twitter.avro", fun simple_hook/2),
    ?assertMatch(
	[{"miguno","Rock: Nerf paper, scissors is fine.",1366150681},
	 {"BlizzardCS","Works as intended.  Terran is IMBA.",1366154481} ], 
       lists:flatten(Data)).

parse_transformers_ocf_test() ->
    Data = eavro:read_ocf("../test/data/transformers.avro"),
    ?assertMatch( 
       {Schema = #avro_record{},
	_Blocks = 
	   [_Block = 
		[ _Rec1 = [<<"Optimus">>,<<"Prime">> |_] | _ ]
	   ]}, Data).


%%
%% Private functions
%%

simple_hook(#avro_record{name='twitter_schema'}, 
	    [Name, Tweet, Timestamp]) ->
    {Name, Tweet, Timestamp};
simple_hook(string, V) -> binary_to_list(V);
simple_hook(_, V) -> V.

    
