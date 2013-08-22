-module(eavro_codec).

-export([encode/2]).

encode(float, Float) when is_float(Float) ->
    <<Float:32/little-float>>;
encode(double, Double) when is_float(Double) ->
    <<Double:64/little-float>>;
encode(boolean, true) -> <<1>>;
encode(boolean, false) -> <<0>>;
encode(null, _Any) -> <<>>.

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
