%%%-------------------------------------------------------------------
%%% @author Vorobyov Vyacheslav <vjache@gmail.com>
%%% @copyright (C) 2014, Vorobyov Vyacheslav
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(eavro_rpc_handler).

-include("eavro.hrl").

%% API

-callback get_protocol() -> 
    #avro_proto{}.

-callback init(Args :: any()) ->
    {ok, State :: any()}.

-callback handle_call( 
	    Call :: { MessageSchema :: #avro_message{}, 
		      CallArgs      :: [ any() ] },
	    State :: any() ) -> 
    {ok,    any()       } |
    {error, avro_type() }.
			 
