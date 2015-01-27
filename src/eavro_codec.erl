-module(eavro_codec).


%% API
-export([ encode/2, 
          decode/2, 
          decode/3 ]).

-export([ varint_encode/1, 
          varint_decode/2 ]).

-include("eavro.hrl").

-define(echo(V), io:format("~p~n",[V])).




%% primitives
encode(int, Int) ->
    Z = zigzag_encode(int, Int),
    varint_encode(<<Z:32>>);
encode(long, Long) ->
    Z = zigzag_encode(long, Long),
    varint_encode(<<Z:64>>);
encode(float, Float) when is_float(Float) ->
    <<Float:32/little-float>>;
encode(double, Double) when is_float(Double) ->
    <<Double:64/little-float>>;
encode(string, Data) when is_binary(Data) ->
    [encode(long, byte_size(Data)), Data];
encode(bytes, Data) when is_binary(Data) ->
    [encode(long, byte_size(Data)), Data];
encode(boolean, true) -> <<1>>;
encode(boolean, false) -> <<0>>;
encode(null, _Any) -> <<>>;



%% complex data types
encode(#avro_record{fields = Fields}, Data) ->
    case Data of
        [[_|_]|_]->
            %unpack deep lists
            FieldDataList=[{#avro_record{fields=Fields},X} || X <-Data],
            [encode(Type, Value) || {Type, Value} <- FieldDataList];
        _ ->
            [encode(Type, Value) || {{_Name, Type}, Value} <- lists:zip(Fields, Data)]
    end;



encode(#avro_enum{symbols = Symbols}, Data) ->
    ZeroBasedIndex = index_of(Data, Symbols) - 1,
    encode(int,ZeroBasedIndex);
encode(#avro_fixed{ size = Size }, Data) ->
    byte_size(Data) == Size orelse exit(bad_size),
    Data;
encode(#avro_map{ values = ValuesType }, Data) when is_list(Data) ->
    encode_blocks(
      ValuesType, Data, 
      fun(T, {K,V}) ->
	[ encode(string, K),
	  encode(T, V) ]
      end );
encode(#avro_array{ items = Type }, Data) when is_list(Data) ->
    encode_blocks(Type, Data, fun encode/2);
encode(Union, {Type, Data}) when is_list(Union) ->
    try 
	I = index_of(Type, Union) - 1,
	[encode(long, I), encode(Type, Data)]
    catch
	_:not_found -> exit({union_mismatch, Union, Type})
    end.

encode_blocks(Type, Data, Encoder) when is_list(Data) ->
    Count = length(Data),
    if Count == 0 -> <<0>>;
       true -> 
	    [encode(long, Count), 
	     [ Encoder(Type, V) || V <- Data],
	     <<0>> ]
    end.
 

index_of(Item, List) -> index_of(Item, List, 1).

index_of(_, [], _)              -> exit(not_found);
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index)   -> index_of(Item, Tl, Index + 1).

%%
%% Decoding functins
%%

decode(Type, Buff ) ->
    decode(Type, Buff, undefined).

-spec decode( Type :: avro_type(), 
              Buff :: binary() | iolist(), 
              Hook :: undefined | decode_hook() ) -> 
    { Value :: term(), Buff :: binary()}.

decode(Type, Buff, Hook) when is_list(Buff) ->
    decode(Type, iolist_to_binary(Buff), Hook);
decode(#avro_record{fields = Fields} = Type, Buff, Hook) ->
    {FieldsValues, Buff1} = 
        lists:foldl(
            fun({_FName, FType}, {Vals0,Buff0}) ->
                {Val, Buff1} = decode(FType, Buff0, Hook),
                {[ Val | Vals0 ], Buff1}
            end, {[], Buff}, Fields),
    { decode_hook(Hook, Type, lists:reverse(FieldsValues) ), Buff1};
decode(#avro_enum{symbols=Symbols} = Type, Buff, Hook) ->
    {ZeroBasedIndex, Buff1} = decode(int, Buff, Hook),
    Symbol = lists:nth(ZeroBasedIndex + 1, Symbols),
    { decode_hook(Hook, Type, Symbol ), Buff1};
decode(#avro_map{values=Type} = CType, Buff, Hook) ->
    decode_blocks(CType, Type, [], Buff, Hook, fun map_entry_decoder/3);
decode(#avro_array{items=Type} = CType, Buff, Hook) ->
    decode_blocks(CType, Type, [], Buff, Hook, fun decode/3);
decode(#avro_fixed{size=Size}=Type, Buff, Hook) ->
    <<Val:Size/binary,Buff1/binary>> = Buff,
    {decode_hook(Hook, Type, Val), Buff1};
decode(Type, Buff, Hook) when Type == string orelse Type == bytes ->
    {ByteSize, Buff1} = decode(long, Buff, undefined),
    <<String:ByteSize/binary, Buff2/binary>> = Buff1,
    {decode_hook(Hook, Type, String), Buff2};
decode(int = Type, Buff, Hook) ->
    {<<Z:32>>, Buff1} = varint_decode(int, Buff),
    Int = zigzag_decode(int, Z),
    {decode_hook(Hook, Type, Int), Buff1};
decode(long = Type, Buff, Hook) ->
    {<<Z:64>>, Buff1} = varint_decode(long, Buff),
    Long = zigzag_decode(long, Z),
    {decode_hook(Hook, Type, Long), Buff1};
decode(float = Type, <<Float:32/little-float,Buff/binary>>, Hook) ->
    {decode_hook(Hook, Type, Float), Buff};
decode(double = Type, <<Double:64/little-float,Buff/binary>>, Hook) ->
    {decode_hook(Hook, Type, Double), Buff};
decode(boolean = Type, <<0:7,B:1,Buff/binary>>, Hook) ->
    {decode_hook(Hook, Type, case B of 0 -> false; 1 -> true end), Buff};
decode(null = Type, Buff, Hook) ->
    {decode_hook(Hook, Type, <<>>), Buff};
decode(Union, Buff, Hook) when is_atom(hd(Union)) ->
    {Idx, Buff1} = decode(long, Buff),
    Type = lists:nth(Idx + 1, Union),
    {Val, Buff2} = decode(Type, Buff1, Hook),
    { {Type, Val}, Buff2}.


map_entry_decoder(Type, Buff, Hook) ->
    {K, Buff1} = decode(string,Buff),
    {V, Buff2} = decode(Type,Buff1,Hook),
    { {K, V}, Buff2}.

decode_blocks(CollectionType, ItemType, Blocks, Buff, Hook, ItemDecoder)->
    %% Decode block item count
    {Count_, Buff1} = decode(long, Buff),
    %% Analyze count: there is a special behavior for count < 0
    {Count, Buff2} = 
	if Count_ < 0  -> 
		%% When count <0 there is a block size, which we are do not use here
		{_BlockSize, Buff_} = decode(long, Buff1),
		{-Count_, Buff_};
	   true -> 
		{Count_, Buff1}
	end,
    %% Decode block items
    {Block, Buff3} = decodeN(Count, ItemType, Buff2, Hook, ItemDecoder),
    case Block of
	[] -> 
	    {decode_hook(Hook, CollectionType, Blocks), Buff3};
	_  ->
	    decode_blocks(CollectionType, ItemType,[Block|Blocks],Buff3,Hook,ItemDecoder)
    end.

decodeN(0, _Type, Buff, _Hook, _Decoder) ->
    {[], Buff};
decodeN(N, Type, Buff, Hook, Decoder) ->
    {H, Buff1}    = Decoder(Type, Buff, Hook),
    {Tail, Buff2} = decodeN(N - 1 , Type, Buff1, Hook, Decoder),
    {[ H | Tail ], Buff2}.

decode_hook(undefined, _Type, Val) ->
    Val;
decode_hook(Hook, Type, Val) when is_function(Hook,2) ->
    Hook(Type, Val).

%% Internal functions

%% ZigZag encode/decode
%% https://developers.google.com/protocol-buffers/docs/encoding?&csw=1#types
zigzag_encode(int, Int) ->
    (Int bsl 1) bxor (Int bsr 31);
zigzag_encode(long, Int) ->
    (Int bsl 1) bxor (Int bsr 63).

zigzag_decode(int, ZigInt) ->
    (ZigInt bsr 1) bxor -(ZigInt band 1);
zigzag_decode(long, ZigInt) ->
    (ZigInt bsr 1) bxor -(ZigInt band 1).


%% Variable-length format
%% http://lucene.apache.org/core/3_5_0/fileformats.html#VInt

%% 32 bit encode
varint_encode(<<0:32>>) -> <<0>>;
varint_encode(<<0:25, B1:7>>) -> <<B1>>;
varint_encode(<<0:18, B1:7, B2:7>>) ->
    <<1:1, B2:7, B1>>;
varint_encode(<<0:11, B1:7, B2:7, B3:7>>) ->
    <<1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:4, B1:7, B2:7, B3:7, B4:7>>) ->
    <<1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<B1:4, B2:7, B3:7, B4:7, B5:7>>) ->
    <<1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;

%% 64 bit encode
varint_encode(<<0:64>>) -> <<0>>;
varint_encode(<<0:57, B1:7>>) -> <<B1>>;
varint_encode(<<0:50, B1:7, B2:7>>) ->
    <<1:1, B2:7, B1>>;
varint_encode(<<0:43, B1:7, B2:7, B3:7>>) ->
    <<1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:36, B1:7, B2:7, B3:7, B4:7>>) ->
    <<1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:29, B1:7, B2:7, B3:7, B4:7, B5:7>>) ->
    <<1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:22, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7>>) ->
    <<1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:15, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7>>) ->
    <<1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:8, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7>>) ->
    <<1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<0:1, B1:7, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7, B9:7>>) ->
    <<1:1, B9:7, 1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>;
varint_encode(<<B1:1, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7, B9:7, B10:7>>) ->
    <<1:1, B10:7, 1:1, B9:7, 1:1, B8:7, 1:1, B7:7, 1:1, B6:7, 1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, B1>>.

varint_decode(int, <<1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 1:1, B2:7, 0:4, B1:4, Bytes/binary>>) ->
    {<<B1:4, B2:7, B3:7, B4:7, B5:7>>, Bytes};
varint_decode(long, <<1:1, B10:7, 1:1, B9:7, 1:1, B8:7, 1:1, B7:7, 
                      1:1, B6:7,  1:1, B5:7, 1:1, B4:7, 1:1, B3:7, 
                      1:1, B2:7, 0:7, B1:1, Bytes/binary>>) ->
    {<<B1:1, B2:7, B3:7, B4:7, B5:7, B6:7, B7:7, B8:7, B9:7, B10:7>>, Bytes};
varint_decode(Type, Bytes) ->
    {DecBits, RestBytes} = varint_decode(Bytes),
    Base = case Type of
        int  -> 32;
        long -> 64
    end,
    LeadingZeroBits = Base - bit_size(DecBits),
    {<<0:LeadingZeroBits/integer, DecBits/bitstring>>, RestBytes}.

varint_decode(<<0:1, X:7, Bytes/binary>>) ->
    {<<X:7>>, Bytes};
varint_decode(<<1:1,X:7, Bytes/binary>>) ->
    {DecBits, Bytes1} = varint_decode(Bytes),
    {<<DecBits/bitstring, X:7>>, Bytes1}.
