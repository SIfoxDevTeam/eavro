%%%-------------------------------------------------------------------
%%% @author Vorobyov Vyacheslav <vjache@gmail.com>
%%% @copyright (C) 2014, Vorobyov Vyacheslav
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(eavro_rpc_test_email_handler).

-behaviour(eavro_rpc_handler).

-include("eavro.hrl").

%% API
-export([get_protocol/0, 
	 init/1,
	 handle_call/2]).

-record(state, { fwd_pid }).

get_protocol() ->
    eavro_rpc_proto:parse_protocol_file(
      "jtest/src/main/avro/mail.avpr").

init(FwdPid) ->
    {ok, #state{fwd_pid = FwdPid} }.

handle_call( {#avro_message{ name = <<"send">> }, 
	      [ _Rec = [_From, _To, Body] ] = _Args}, 
	     #state{fwd_pid = FwdPid} = _State ) ->
    if FwdPid /= undefined ->
	    FwdPid ! {?MODULE, _Rec};
       true -> ok
    end,
    case Body of
	<<"EXIT">> ->
	    exit({test_error, "Some reason"});
	<<"ERROR">> -> 
	    {error, <<"ERROR!">>};
	_ ->
	    {ok, <<"OK!">>}
    end.
		   
		   
