-module(eavro_rpc_proto_tests).

-include("eavro.hrl").

-include_lib("eunit/include/eunit.hrl").


-define(assertMatch_(Exp), fun(Val) -> ?assertMatch(Exp, Val) end).

parse_flume_proto_test() ->
    Proto = eavro_rpc_proto:parse_protocol_file("../test/data/flume.avpr"),
    ?assertMatch(
       #avro_proto{ ns       = <<"org.apache.flume.source.avro">>,
		    name     = <<"AvroSourceProtocol">>,
		    types    = [_ | _],
		    messages = [#avro_message{} | _],
		    json     = <<_,_/binary>>}, Proto).

call_email_eserver_test_() ->
    %% 1. Start Erlang RPC Server 
    eavro_rpc_srv:start(
      eavro_rpc_test_email_handler,self(),2525,1),
    {ok, Cli} = eavro_rpc_fsm:start_link(
		"localhost", 2525, "../jtest/src/main/avro/mail.avpr"),
    %% 2. Do calls using 'eavro' erlang client
    ECalls = [{ Title,
       fun() ->
	       Ret = eavro_rpc_fsm:call(
		       Cli, send, 
		       _Args = [ _Rec = [ <<"TOOOO">>, <<"FROOOOOM">>, 
					  list_to_binary(MsgBody) ] ]),
	       RetExpect(Ret)
       end} || {Title, MsgBody, RetExpect} <- 
		   [ { "e-send 'HELLO'", "HELLO", ?assertMatch_({ok, <<"OK!">>}) },
		     { "e-send 'EXIT'", "EXIT", 
		       ?assertMatch_({error, 
				      {string, 
				       <<"Server error",_/binary>>} }) },
		     { "e-send 'ERROR'", "ERROR", 
		       ?assertMatch_({error, {string, <<"ERROR!">>} }) } ] ],
    JCalls = [{ "j-send ", 
		with_port(
		  "client 2525",
		  fun() -> ok end) }],
    {inorder, ECalls ++ JCalls}.

call_email_jserver_test_() ->
    {timeout, 30, 
     with_port(
       "server 65111",
       fun() ->
	       {ok, P} = eavro_rpc_fsm:start_link(
			   "localhost", 65111, "../jtest/src/main/avro/mail.avpr"),
	       Ret = eavro_rpc_fsm:call(
		       P, send, 
		       _Args = [ _Rec = [ <<"TOOOO">>, <<"FROOOOOM">>, <<"HELLO">> ] ]),
	       ?assertMatch({ok, <<"Sending ",_/binary>>}, Ret)
       end)}.

with_port(Args, Fun) ->
    fun() ->
	    Port = erlang:open_port(
		     {spawn, "java -jar ../jtest/target/eavro-rpc-test-servers-1.7.5-SNAPSHOT-jar-with-dependencies.jar " ++ Args},
		     [{cd, "../jtest"},
		      {env, []},
		      binary,
		      stream,
		      {parallelism, true},
		      exit_status]),
	    try 
		ok = await_done(Port),
		Fun()
	    after
		erlang:port_close(Port)
	    end
    end.
    
await_done(Port) ->
    receive
	{Port, {data, <<"*DONE*",_/binary>>}} -> ok;
	{Port, {data, _}} ->
	    await_done(Port)
    end.
