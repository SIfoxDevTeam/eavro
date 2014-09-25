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
-export([start/4, start_link/4]).

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
		handshaked = false,
		c_module :: atom(),
		c_state :: any()}).

%% API.

-define(echo(M), io:format("~p~n", [M])).


start(CallbackModule,
      CallbackOpts,
      Port, 
      NumAcceptors) when is_atom(CallbackModule), 
			 is_integer(Port), 
			 is_integer(NumAcceptors) ->
    case application:start(ranch) of
	ok                              -> ok;
	{error,{already_started,ranch}} -> ok
    end,
    Proto = CallbackModule:get_protocol(),
    ranch:start_listener(
      ?MODULE, NumAcceptors,
      ranch_tcp, [{port, Port}],
      ?MODULE, 
      [{proto, Proto},
       {callback_module, CallbackModule},
       {callback_opts, CallbackOpts}]).

%% Ranch Protocol Handler Behaviour Callback Function
start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [{Ref, Socket, Transport, Opts}]).

%% gen_server.
init({Ref, Socket, Transport, Opts}) ->
    #avro_proto{} = Proto = 
	proplists:get_value(proto, Opts),
    {CModule, CState} = callback_init(Opts),
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}]),
    {cont, Cont} = eavro_rpc_proto:decode_frame_sequences(<<>>),
    gen_server:enter_loop(
      ?MODULE, [],
      #state{ socket    = Socket, 
	      transport = Transport, 
	      proto     = Proto,
	      cont      = Cont,
	      c_module  = CModule,
	      c_state   = CState},
      ?TIMEOUT).

handle_info({tcp, Soc, Data}, 
	    #state{ socket     = Soc,
		    proto      = Proto,
		    handshaked = Handshaked,
		    cont       = Cont } = State) ->
    active_once(State),
    case Cont(Data) of
	{cont, Cont1} -> 
	    State1 = State;
	{ [{Ser0, [ HeadFrame | Frames ]} = _HeadSeq | Sequences] = AsIs, 
	  {cont, Cont1} } ->
	    case Handshaked of
		false ->
		    { _HSReq, HeadFrameTail } =
			eavro_rpc_proto:decode_handshake_request(HeadFrame),
		    HRes = eavro_rpc_proto:encode_handshake_response(Proto),
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
do_calls([ {Ser, Frames} | Seq], 
	 #state{ proto = Proto } = State) ->
    {{_MsgSchema, _Args} = Call, Rest} = 
	eavro_rpc_proto:decode_call(Proto, iolist_to_binary(Frames)),
    <<>> = iolist_to_binary(Rest),
    EncResp = do_call(Call, State),
    [{Ser, [EncResp]} | do_calls(Seq, State)].

do_call({#avro_message{} = MsgSchema, _Args} = Call,
	#state{	 c_module = CMod, 
		 c_state  = CState } = _State) ->
    Ret =
	try CMod:handle_call(Call, CState) of
	    {ok,    _} = R -> R;
	    {error, _} = R -> R;
	    Bad -> {error, iolist_to_binary(
			     io_lib:format("Server error[1]: ~p", 
					   [{handler_bad_ret,Bad}]))}
	catch _:Reason ->
		{error, iolist_to_binary(
			  io_lib:format("Server error[2]: ~p", 
					[{handler_exit, Reason}]))}
	end,
    try eavro_rpc_proto:encode_response(MsgSchema, Ret)
    catch 
	_:Reason1 ->
	    eavro_rpc_proto:encode_response(
	      MsgSchema, 
	      {error, 
	       iolist_to_binary(
		 io_lib:format("Server error[3]: ~p", 
			       [{handler_result_encode_failed, Reason1}]))})
    end.
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

callback_init(Opts) ->
    CMod = proplists:get_value(callback_module, Opts),
    CMod /= undefined orelse exit(no_callback_module),
    COpts = proplists:get_value(callback_opts, Opts, []),
    {ok, CState} = CMod:init(COpts),
    {CMod, CState}.


active_once(#state{ socket    = Soc, 
		    transport = Tsp}) ->
    Tsp:setopts(Soc, [{active, once}]).

tcp_reply(#state{ socket    = Soc, 
		    transport = Tsp}, Data) ->
    Tsp:send(Soc, Data).

-spec make_frame( Data :: iolist()) -> iolist().
make_frame(Data) ->
    Size = iolist_size(Data),
    [<<Size:32>>, Data].

make_frame_sequence(Serial, EncodedCalls) when is_integer(Serial), 
					       is_list(EncodedCalls) ->
    SequenceLength = length(EncodedCalls),
    [<<Serial:32, SequenceLength:32>> | [ make_frame(Call) || Call <- EncodedCalls ] ].
