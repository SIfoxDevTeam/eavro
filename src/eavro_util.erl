-module(eavro_util).

-include("eavro.hrl").

-export([std_hook/2]).

std_hook(#avro_array{}, Blocks) ->
    [ Val || Block <- Blocks, Val <- Block ];
std_hook(#avro_map{}, Blocks) ->
    [ Val || Block <- Blocks, Val <- Block ];
std_hook(_,V) -> V.
