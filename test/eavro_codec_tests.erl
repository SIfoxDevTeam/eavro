-module(eavro_codec_tests).

-include("eavro.hrl").

-include_lib("eunit/include/eunit.hrl").

%%====================================================================================================
%% TESTS
%%====================================================================================================

integer_codec_test_() ->
    {inorder,
     [{caption( "Encode/Decode ~p as '~s'.", [N, Type]),
       fun() ->
	       ?assertMatch( 
		  {N, <<>>}, 
		  eavro_codec:decode(Type, eavro_codec:encode(Type, N)) )
       end} ||  {Type, Base, Gen} <- [ {int, 32, fun generate_int32_test_values/0}, 
				       {long, 64, fun generate_int64_test_values/0}],
		<<N:Base/signed-integer>> <- Gen() ]}.

varint_codec_test_() ->
    {inorder,
         [{"Check varint codec of int32 when no leading zeroes.",
           fun() ->
            FF32 = 16#FFFFFFFF,
            EncBytes = <<_:5/binary>> = eavro_codec:varint_encode(<<FF32:32>>),
            {<<FF32:32>>, <<>>} = eavro_codec:varint_decode(int, EncBytes)
           end},
           {"Check varint codec of int64 when no leading zeroes.",
             fun() ->
                FF64 = 16#FFFFFFFFFFFFFFFF,
                EncBytes = <<_:10/binary>> = eavro_codec:varint_encode(<<FF64:64>>),
                {<<FF64:64>>, <<>>} = eavro_codec:varint_decode(long, EncBytes)
             end},
           {"Check varint codec of int32 when no leading zeroes with some irregularity in first byte.",
             fun() ->
                Int32    = <<2#10101110:8,16#FFFFFF:24>>,
                EncBytes = <<_:5/binary>> = eavro_codec:varint_encode(Int32),
                {Int32, <<>>} = eavro_codec:varint_decode(int, EncBytes)
             end},
           {"Check varint codec of int64 when no leading zeroes with some irregularity in first byte.",
             fun() ->
                Int64   = <<2#10101110:8,16#FFFFFFFFFFFFFFFF:56>>,
                EncBytes = <<_:10/binary>> = eavro_codec:varint_encode(Int64),
                {Int64, <<>>} = eavro_codec:varint_decode(long, EncBytes)
             end}] ++ 
             [ {caption("Check varint codec of int32: ~p.", [Int32]),
               fun() ->
                EncBytes = eavro_codec:varint_encode(Int32),
                {Int32, <<>>} = eavro_codec:varint_decode(int, EncBytes)
               end}|| Int32 <- generate_int32_test_values()] ++
             [{caption("Check varint codec of int64: ~p.", [Int64]),
              fun() ->
                 EncBytes = eavro_codec:varint_encode(Int64),
                 {Int64, <<>>} = eavro_codec:varint_decode(long, EncBytes)
              end} || Int64 <- generate_int64_test_values()]}.

%%
%% Test app domain record.
%%
-record('Person', { id, fname, lname, age, is_autobot, 
		    is_desepticon, energy_level, home, clan}).
-record('GalaxyCoord', {x,y,z}).

avro_record_codec_test_() ->
    {inorder, 
     [{ "Encode/Decode avro record with all types of fields and nested avro record.",
	fun()->
		Type = #avro_record{ 
			  name   = 'Person', 
			  fields = [{id, long},
				    {energy_level, float},
				    {home, #avro_record{ 
					      name = 'GalaxyCoord', 
					      fields = [{x,double}, 
							{y,double}, 
							{z,double}] } },
				    {fname, string}, 
				    {lname, string}, 
				    {age, int}, 
				    {is_autobot, boolean}, 
				    {is_desepticon, boolean},
				    {clan, #avro_enum{
					      name    = 'Clan', 
					      symbols = ['Autobots', 'Desepticons']} }
				   ]},
		Hook = fun(#avro_record{ name = 'Person'} = _Type, 
			   [Id, Ener, Home, Fname, Lname, Age, IsAu, IsDe, Clan]) -> 
			       #'Person'{id = Id, energy_level = Ener, home = Home, 
					 fname = Fname, lname = Lname, age = Age, 
					 is_autobot = IsAu, is_desepticon = IsDe, 
					 clan = Clan};
			  (#avro_record{ name = 'GalaxyCoord'} = _Type, 
			   [X,Y,Z]) -> #'GalaxyCoord'{x = X, y = Y, z = Z};
			  ( string, B) -> binary_to_list(B);
			  (_, AsIs) -> AsIs
		       end,
		GalaxyCoord = [X,Y,Z] = [ 2342.34523, 
					  675322341.422422324, 
					  242252.56473457345],
		Encoded = eavro_codec:encode(
			    Type, [ 16#AABBCCDDEEFF0011, 23423.5674 , 
				    GalaxyCoord, 
				    <<"Optimus">>, <<"Prime">>, 1000, true, false, 
				    'Autobots']),
		Decoded = eavro_codec:decode(Type, Encoded, Hook), 
		?assertMatch( {#'Person'{ fname = "Optimus", 
					  lname = "Prime", 
					  age   = 1000,
					  home  = #'GalaxyCoord'{ x = X, y = Y, z = Z},
					  clan  = 'Autobots',
					  is_autobot = true,
					  is_desepticon = false }, <<>>}, Decoded)
	end}
     ]}.

avro_map_codec_test() ->
    Type = #avro_map{values = long},
    Map = lists:sort([ {<<"k1">>, 1}, {<<"k2">>, 2}, {<<"k3">>, 3} ]),
    Encoded = eavro_codec:encode(Type, Map),
    { DecodedMapBlocks, <<>>} = eavro_codec:decode(Type, Encoded), 
    DecodedMap = lists:flatten(DecodedMapBlocks),
    ?assertMatch( Map, lists:sort(DecodedMap)).

avro_fixed_codec_test() ->
    Type = #avro_fixed{size = 5},
    Fixed = <<0,1,2,3,4>>,
    Encoded = eavro_codec:encode(Type, Fixed),
    ?assertMatch({ Fixed, <<>>}, eavro_codec:decode(Type, Encoded) ).

avro_array_codec_test() ->
    Type = #avro_array{items = string},
    Array = [<<"Alpha">>, <<"Beta">>, <<"Gamma">>, <<"Delta">>, <<"Epsilon">>, <<"Dzeta">>],
    Encoded = eavro_codec:encode(Type, Array),
    ?assertMatch({ [Array], <<>>}, eavro_codec:decode(Type, Encoded) ).

avro_union_codec_test() ->
    Union = [int,string],
    ?assertMatch({ 137, <<>>}, 
		 eavro_codec:decode(
		   Union, 
		   eavro_codec:encode(Union, {int, 137}) ) ),
    ?assertMatch({ <<"NaN">>, <<>>}, 
		 eavro_codec:decode(
		   Union, 
		   eavro_codec:encode(Union, {string, <<"NaN">>}) ) ).

avro_array_of_union_codec_test() ->
    RecType = #avro_record{ name = some_struct, 
			    fields = [{field1, long}]},
    Type = #avro_array{ 
	      items = 
		  [int, string, RecType] },
    ?assertMatch({ [ _Block = [ 1, 2, <<"very much">>, [137] ] ], <<>>}, 
		 eavro_codec:decode(
		   Type, 
		   eavro_codec:encode(
		     Type, 
		     [ {int, 1}, 
		       {int, 2}, 
		       {string, <<"very much">>}, 
		       {RecType, [137]} ]) ) ).

%%====================================================================================================
%% HELPER FUNCTIONS
%%====================================================================================================

caption(Fmt, Args) ->
	lists:flatten(io_lib:format(Fmt, Args)).

generate_int32_test_values() ->
    [<<0:32>>]++[ <<(2#11001000 bsl (N*8)):32>>|| N <- lists:seq(0,3)].

generate_int64_test_values() ->
    [<<0:64>>] ++ [ <<(2#11001000 bsl (N*8)):64>>|| N <- lists:seq(0,7)].
