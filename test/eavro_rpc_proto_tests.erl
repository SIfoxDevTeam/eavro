-module(eavro_rpc_proto_tests).

-include("eavro.hrl").

-include_lib("eunit/include/eunit.hrl").

parse_flume_proto_test() ->
    Proto = eavro_rpc_proto:parse_protocol_file("../test/data/flume.avpr"),
    ?assertMatch(
       #avro_proto{ ns       = <<"org.apache.flume.source.avro">>,
		    name     = <<"AvroSourceProtocol">>,
		    types    = [_ | _],
		    messages = [#avro_message{} | _],
		    json     = <<_,_/binary>>}, Proto).

call_email_eserver_test() ->
    eavro_rpc_srv:start(
      eavro_rpc_test_email_handler,self(),2525,1),
    {ok, P} = eavro_rpc_fsm:start_link(
		"localhost", 2525, "../jtest/src/main/avro/mail.avpr"),
    OkRet = eavro_rpc_fsm:call(
	      P, send, 
	      _Args = [ _Rec = [ <<"TOOOO">>, <<"FROOOOOM">>, <<"HELLO">> ] ]),
    ?assertMatch({ok, <<"OK!">>}, OkRet),
    ErrRet = eavro_rpc_fsm:call(
	       P, send, 
	       _Args1 = [ _Rec1 = [ <<"TOOOO">>, <<"FROOOOOM">>, <<"EXIT">> ] ]),
    ?assertMatch({error, {string, <<"Server error",_/binary>>} }, ErrRet),
    ErrRet1 = eavro_rpc_fsm:call(
		P, send, 
		_Args2 = [ _Rec2 = [ <<"TOOOO">>, <<"FROOOOOM">>, <<"ERROR">> ] ]),
    ?assertMatch({error, {string, <<"ERROR!">>} }, ErrRet1).

call_email_jserver_test_() ->
    {timeout, 30, 
     fun()->
	     Port = erlang:open_port(
		      {spawn, "java -jar ../jtest/target/eavro-rpc-test-servers-1.7.5-SNAPSHOT-jar-with-dependencies.jar email.Main"},
		      [{cd, "../jtest"},
		       {env, []},
		       binary,
		       stream,
		       {parallelism, true},
		       exit_status]),
	     try
		 ok = await_start_finished(Port),
		 {ok, P} = eavro_rpc_fsm:start_link(
			     "localhost", 65111, "../jtest/src/main/avro/mail.avpr"),
		 Ret = eavro_rpc_fsm:call(
			 P, send, 
			 _Args = [ _Rec = [ <<"TOOOO">>, <<"FROOOOOM">>, <<"HELLO">> ] ]),
		 ?assertMatch({ok, <<"Sending ",_/binary>>}, Ret)
	     after
		 erlang:port_close(Port)
	     end
     end}.
    
await_start_finished(Port) ->
    receive
	{Port, {data, <<"Server started",_/binary>>}} -> ok;
	{Port, {data, _}} ->
	    await_start_finished(Port)
    end.
