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
