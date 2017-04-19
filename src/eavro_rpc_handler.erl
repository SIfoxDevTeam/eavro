-module(eavro_rpc_handler).

-include("eavro.hrl").

%%
%% Returns an Avro protocol served by this Avro RPC server.
%% Use e.g. :
%%   eavro_rpc_proto:parse_protocol_file("my_proto.avpr")
%%
-callback get_protocol() ->
    #avro_proto{}.

%%
%% This function called when client established connection
%% with a server. Implementing this call gives handler a chance
%% for some initializations.
%%
-callback init(Args :: any()) ->
    {ok, State :: any()}.

%%
%% Implement this callback to handle client calls.
%%
-callback handle_call(
	    Call :: { MessageSchema :: #avro_message{},
		      CallArgs      :: [ any() ] },
	    State :: any() ) ->
    {ok,    any()       } |
    {error, avro_type() }.

