-module(eavro_rpc_proto).

-compile(export_all).

-include("eavro.hrl").

-export([parse_protocol_file/1,
	 parse_protocol/1,
	 encode_call/3,
	 decode_call/2,
	 encode_response/2,
	 encode_handshake_response/1,
	 decode_frame_sequences/1,
	 decode_handshake_response/1,
	 decode_handshake_request/1,
	 decode_call_response/2]).

%% Parses Avro Protocol Definition from JSON file (*.avpr).
-spec parse_protocol_file(Filename :: file:filename()) -> #avro_proto{}.
parse_protocol_file(Filename) ->
    {ok, Json} = file:read_file(Filename),
    parse_protocol(Json).

%%
%% Parses Avro Protocol Definition from JSON formatted text..
%%
-spec parse_protocol(AvprJson :: binary()) -> #avro_proto{}.
parse_protocol(Json) ->
    Jsx = [{_,_}|_] = jsx:decode(Json),
    Prop = fun(Name, Props) ->
		   case proplists:get_value(Name, Props) of
		       undefined ->
			   exit({expected, Name});
		       Val -> Val
		   end
	   end,
    Messages         = Prop(<<"messages">>, Jsx),
    {Types, Context} = eavro:parse_types(Prop(<<"types">>, Jsx), dict:new()),
    Name             = Prop(<<"protocol">>, Jsx),
    Ns               = Prop(<<"namespace">>, Jsx),
    ParseMessage =
	fun({MName, M}) ->
		Req = Prop(<<"request">>, M),
		Res = Prop(<<"response">>, M),
		ResolveType =
		    fun(ArgNameOrDef) ->
			  try {TypeParsed, _Context1} =
				   eavro:parse_type(ArgNameOrDef, Context),
			       TypeParsed
			  catch
			      _:_ when is_binary(ArgNameOrDef) ->
				  ArgName = binary_to_atom(ArgNameOrDef,latin1),
				  hd([_] = [ T || T <- Types,
						  case T of
						      #avro_record{name = ArgName} -> true;
						      #avro_enum{name   = ArgName} -> true;
						      #avro_fixed{name  = ArgName} -> true;
						      _ -> false
						  end])
			  end
		    end,
		MArgs =
		    [ ResolveType(Prop(<<"type">>, Arg)) || Arg <- Req ],
		#avro_message{
		    name = MName,
		    args = MArgs,
		    return = ResolveType(Res) }
	end,
    #avro_proto{
       ns       = Ns,
       name     = Name,
       types    = Types,
       messages = lists:map(ParseMessage, Messages),
       json     = Json }.

%%
%% Encoding/decoding calls.
%%

encode_call(Proto,
	    MessageName, Args) when is_atom(MessageName) ->
    encode_call(Proto, atom_to_binary(MessageName,latin1), Args);
encode_call(#avro_proto{} = Proto,
	    MessageName, Args) when is_binary(MessageName), is_list(Args) ->
    #avro_message{
       name = MessageNameBin,
       args = Types } = get_message(Proto,  MessageName),
    length(Args) == length(Types) orelse exit(bad_message_arity),
    [eavro:encode(schema_Meta(), []),
     eavro:encode(string, MessageNameBin),
     [ eavro:encode(Type, Arg) || {Type, Arg} <- lists:zip(Types, Args) ]
    ].

decode_call(#avro_proto{} = Proto, Buff) ->
    {_Meta, Buff1} = eavro:decode(schema_Meta(), Buff),
    {MessageNameBin, Buff2} = eavro:decode(string, Buff1),
    Msg = #avro_message{
	     args = Types } = get_message(Proto,  MessageNameBin),
    {ArgsR, Buff3} =
	lists:foldl(
	  fun(T, {Vals, B}) ->
		  {V, B1} = eavro:decode(T,B),
		  {[V|Vals], B1}
	  end,
	  {[], Buff2},
	  Types),
    { {Msg, lists:reverse(ArgsR)}, Buff3}.

encode_response(#avro_proto{} = Proto, MessageName, Result) ->
    M = get_message(Proto,  MessageName),
    encode_response(M, Result).

encode_response(#avro_message{return = RetType }, {Status, Ret}) ->
    [eavro:encode(schema_Meta(), []),
     case Status of
	 ok -> [<<0>>, eavro:encode(RetType, Ret)];
	 error -> [<<1>>, eavro:encode(
			    [string, RetType],
			    {RetType, Ret} )]
     end].

encode_handshake_response(#avro_proto{json = Json})->
    eavro:encode(
      schema_HandshakeResponse(),
      ['CLIENT',
       {string, Json},
       {schema_MD5(), erlang:md5(Json)},
       {schema_Meta(), []}]).

get_message(#avro_proto{ messages = Messages},
	    MessageName) ->
    case lists:keyfind(if is_atom(MessageName) -> atom_to_binary(MessageName, latin1);
			  true -> MessageName
		       end, #avro_message.name, Messages) of
	#avro_message{} = Msg -> Msg;
	false -> exit({unsupported_proto_message, MessageName})
    end.

decode_response(Proto, MessageName, Buff) ->
    #avro_message{return = RetType } = get_message(Proto,  MessageName),
    [{Ser, Buff1} | Buffs] = decode_frame_sequences(Buff),
    {HeaderResponse, Buff2} = decode_handshake_response(Buff1),
    {HeaderResponse,
     [ {S, decode_call_response(RetType, B)} || {S,B} <- [ {Ser, Buff2} | Buffs ] ] }.

decode_call_response(RetType, Buff) ->
    {_Meta, <<ErrFlag,Buff1/binary>>} = eavro:decode(schema_Meta(), Buff),
    case ErrFlag of
	0 ->
	    {Rsp, <<>>} = eavro:decode(RetType,Buff1),
	    {ok, Rsp};
	1 ->
	    {Err, <<>>} = eavro:decode([string, RetType],Buff1),
	    {error, Err}
    end.

test_data() ->
    [<<0:32>>,<<2:32>>,<<3:32,1,2,3>>,<<4:32,1,2,3,4>>].

decode_frame_sequences(<<Serial:32,N:32,Tail/binary>>) ->
    continue_decode_frame_sequences(Serial,
      decode_frame_sequence(N, Tail, []));
decode_frame_sequences(Buff) ->
    {cont,
     fun(Buff1) ->
	     decode_frame_sequences(<<Buff/binary, Buff1/binary>>)
     end}.

continue_decode_frame_sequences(Serial, {cont, Cont}) ->
    {cont,
     fun(Buff1) ->
	     continue_decode_frame_sequences(Serial, Cont(Buff1))
     end};
continue_decode_frame_sequences(Serial, {Arr, Tail2}) ->
    case decode_frame_sequences(Tail2) of
	{cont, _} = Cont -> { [ {Serial, Arr} ], Cont};
	{Arrs, Cont}     -> { [ {Serial, Arr} | Arrs], Cont}
    end.



decode_frame_sequence(0, Buff, Acc) ->
    {lists:reverse(Acc), Buff};
decode_frame_sequence(N, <<S:32, Tail/binary>> = Buff, Acc) ->
    case Tail of
	<<F:S/binary, Tail1/binary>> ->
	    decode_frame_sequence(N - 1, Tail1, [F|Acc]);
	_ ->
	    {cont, fun(Buff1) ->
		    decode_frame_sequence(
		      N, <<Buff/binary, Buff1/binary>>, Acc)
	    end}
    end;
decode_frame_sequence(N, Buff, Acc) ->
    {cont, fun(Buff1) ->
	    decode_frame_sequence(
	      N, <<Buff/binary, Buff1/binary>>, Acc)
    end}.


decode_handshake_response(Buff) ->
    eavro:decode(schema_HandshakeResponse(), Buff).

decode_handshake_request(Buff) ->
    eavro:decode(schema_HandshakeRequest(), Buff).



%%
%% Avro Protocol Handshake Schema's
%%

schema_HandshakeRequest() ->
    #avro_record{
       name = 'HandshakeRequest',
       fields = [{clientHash, schema_MD5()},
		 {clientProtocol, [null, string]},
		 {serverHash, schema_MD5()},
		 {meta, [null,schema_Meta()]}]}.

schema_HandshakeResponse() ->
    #avro_record{
       name = 'HandshakeResponse',
       fields = [{match, #avro_enum{ name    = 'HandshakeMatch',
				     symbols = ['BOTH', 'CLIENT', 'NONE']}},
		 {serverProtocol, [null, string]},
		 {serverHash, [null, schema_MD5()]},
		 {meta, [null, schema_Meta()]}]}.

schema_Meta() ->
    #avro_map{ values = bytes }.

schema_MD5() ->
    #avro_fixed{ name = 'MD5', size = 16}.
