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

call_email_server_test_() ->
    {timeout, 30, 
     fun()->
	     Port = erlang:open_port(
		      {spawn, "mvn exec:java -Dexec.mainClass=\"email.Main\""},
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
