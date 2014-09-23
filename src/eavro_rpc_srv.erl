%%%-------------------------------------------------------------------
%%% @author Vorobyov Vyacheslav <vjache@gmail.com>
%%% @copyright (C) 2014, Vorobyov Vyacheslav
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(eavro_rpc_srv).

-include("eavro.hrl").

-behaviour(gen_server).
-behaviour(ranch_protocol).

%% API
-export([start/0, start_link/4]).

%% gen_server callbacks
-export([init/1, 
	 handle_call/3, 
	 handle_cast/2, 
	 handle_info/2,
	 terminate/2, 
	 code_change/3]).

-define(SERVER, ?MODULE).
-define(TIMEOUT, 30000).

-record(state, {socket, 
		transport,
		proto :: #avro_proto{},
		cont,
		serial = 0,
		handshaked = false}).

%% API.

-define(echo(M), io:format("~p~n", [M])).

start() ->
    {ok, _} = application:ensure_all_started(ranch),
    Port = 2525,
    NumAcceptors = 1,
    ranch:start_listener(
      ?MODULE, NumAcceptors,
      ranch_tcp, [{port, Port}],
      ?MODULE, []).

start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [{Ref, Socket, Transport, Opts}]).

%% gen_server.
init({Ref, Socket, Transport, Opts}) ->
    ?echo({'START'}),
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}]),
    #avro_proto{} = Proto = 
	eavro_rpc_proto:parse_protocol_file(
	  "jtest/src/main/avro/mail.avpr"),%proplists:get_value(proto, Opts),
    {cont, Cont} = eavro_rpc_proto:decode_frame_sequences(<<>>),
    gen_server:enter_loop(
      ?MODULE, [],
      #state{ socket    = Socket, 
	      transport = Transport, 
	      proto     = Proto,
	      cont      = Cont      },
      ?TIMEOUT).

active_once(#state{ socket    = Soc, 
		    transport = Tsp}) ->
    Tsp:setopts(Soc, [{active, once}]).

tcp_reply(#state{ socket    = Soc, 
		    transport = Tsp}, Data) ->
    Tsp:send(Soc, Data).

handle_info({tcp, Soc, Data}, 
	    #state{ socket     = Soc,
		    proto      = Proto,
		    handshaked = Handshaked,
		    cont       = Cont } = State) ->
    active_once(State),
    case Cont(Data) of
	{cont, Cont1} -> 
	    ?echo({inc_rcv}),
	    State1 = State;
	{ [{Ser0, [ HeadFrame | Frames ]} = _HeadSeq | Sequences] = AsIs, 
	  {cont, Cont1} } ->
	    ?echo({seq_rcv, AsIs}),
	    case Handshaked of
		false ->
		    { _HSReq, HeadFrameTail } =
			eavro_rpc_proto:decode_handshake_request(HeadFrame),
		    ?echo({handshake_req, _HSReq}),
		    HRes = eavro_rpc_proto:encode_handshake_response(Proto),
		    ?echo({handshake_res, HRes}),
		    [{Ser0, Fs0} | Seqs] = 
			do_calls(
			  [{Ser0, [HeadFrameTail|Frames]} | Sequences],
			  State),
		    Replies = [{Ser0, [HRes | Fs0]} | Seqs];
		true ->
		    Replies = do_calls( AsIs, State)
	    end,
	    [ tcp_reply(
		State, 
		make_frame_sequence(Ser, Fs)) || {Ser, Fs} <- Replies ],
	    State1 = State#state{ handshaked = true}
    end,
    {noreply, State1#state{ cont = Cont1 } };
handle_info({tcp_closed, _Socket}, State) ->
		   {stop, normal, State};
handle_info({tcp_error, _, Reason}, 
	    State)          -> {stop, Reason, State};
handle_info(timeout, State) -> {stop, normal, State};
handle_info(_Info, State)   -> {stop, normal, State}.

do_calls([], _State) ->
    [];
do_calls([ {Ser, Frames} | Seq], #state{ proto = Proto} = State) ->
    {{AvMsgSchema, _Args} = Call, Rest} = 
	eavro_rpc_proto:decode_call(Proto, iolist_to_binary(Frames)),
    <<>> = iolist_to_binary(Rest),
    {Status, _} = Ret = do_call(Call, State),
    Status == ok orelse Status == error orelse exit({bad_return, Ret}),
    EncResp = eavro_rpc_proto:encode_response(AvMsgSchema, Ret ),
    [{Ser, [EncResp]} | do_calls(Seq, State)].

do_call({#avro_message{} = _MsgSchema, _Args},
	_State) ->
    
    {ok, <<"OKKK">>}.
%% --------------------------------------------------------------------
%% Unused behaviour callbacks
%% --------------------------------------------------------------------
handle_call(_Request, _From, State) -> {reply, ok, State}.
handle_cast(_Msg, State)            -> {noreply, State}.
terminate(_Reason, _State)          -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% --------------------------------------------------------------------
%% Private
%% --------------------------------------------------------------------
-spec make_frame( Data :: iolist()) -> iolist().
make_frame(Data) ->
    Size = iolist_size(Data),
    [<<Size:32>>, Data].

make_frame_sequence(Serial, EncodedCalls) when is_integer(Serial), 
					       is_list(EncodedCalls) ->
    SequenceLength = length(EncodedCalls),
    [<<Serial:32, SequenceLength:32>> | [ make_frame(Call) || Call <- EncodedCalls ] ].
