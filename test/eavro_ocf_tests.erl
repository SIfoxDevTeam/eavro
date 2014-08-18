-module(eavro_ocf_tests).

-include("eavro.hrl").

-include_lib("eunit/include/eunit.hrl").

parse_twitter_ocf_test() ->
    Data = eavro:read_ocf("../test/data/twitter.avro"),
    ?assertMatch( 
       _Blocks = 
	   [_Block = 
		[ _Rec1 = [<<"miguno">>,<<"Rock: Nerf paper, scissors is fine.">>,
			   1366150681],
		  _Rec2 = [<<"BlizzardCS">>,<<"Works as intended.  Terran is IMBA.">>,
			   1366154481]
		]
	   ], Data).

parse_twitter_ocf_with_hook_test() ->
    Data = lists:flatten(
	     eavro:read_ocf("../test/data/twitter.avro", fun simple_hook/2)),
    ?assertMatch(
       [{<<"miguno">>,<<"Rock: Nerf paper, scissors is fine.">>,
	 1366150681},
	{<<"BlizzardCS">>,<<"Works as intended.  Terran is IMBA.">>,
	 1366154481} ], 
       Data).

simple_hook(#avro_record{name='twitter_schema'}, 
	    [Name, Tweet, Timestamp]) ->
    {Name, Tweet, Timestamp};
simple_hook(_, V) -> V.

    
