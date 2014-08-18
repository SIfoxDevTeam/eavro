-module(eavro).

%% API
-export([schema/1]).
-export([encode/2]).

-include("eavro.hrl").

schema(File) ->
    case file:read_file(File) of
        {ok, Data} ->
            Schema = jsx:decode(Data),
            parse_schema(Schema);
        Error -> Error
    end.

encode(Schema, Data) ->
    iolist_to_binary(eavro_codec:encode(Schema, Data)).

%%
%% Private functions section
%%

parse_schema(Schema) ->
    parse_type(Schema).

parse_type(Simple) when is_binary(Simple) ->
    case Simple of
	<<"null">>    -> null;
	<<"boolean">> -> boolean;
	<<"int">>     -> int;
	<<"long">>    -> long;
	<<"double">>  -> double;
	<<"string">>  -> string;
	<<"bytes">>   -> bytes;
	BadType       -> exit({bad_simple_type, BadType})
    end;
parse_type([{_,_}|_] = Complex) ->
    Parser = 
	case proplists:get_value(<<"type">>,Complex) of
	    <<"record">> ->
		fun parse_record/1;
	    <<"enum">> ->
		fun parse_enum/1;
	    <<"union">> ->
		fun parse_union/1;
	    <<"map">> ->
		fun parse_map/1;
	    <<"array">> ->
		fun parse_array/1;
	    <<"fixed">> ->
		fun parse_fixed/1;
	    BadType -> exit({bad_complex_type, BadType})
	end,
    Parser(Complex);
parse_type(_) -> exit(badarg).


get_attributes(Complex, Attrs) ->
    [proplists:get_value(Attr, Complex) || Attr <- Attrs].

binary_to_latin1_atom(Bin) ->
    binary_to_atom(Bin,latin1).

parse_record(Record) ->
    [Name, Fields] = get_attributes(Record, [<<"name">>, <<"fields">>]),
    #avro_record{ name   = binary_to_latin1_atom(Name), %% From Avro spec.: [A-Za-z0-9_]
		  fields = lists:keymap(fun parse_type/1, 2, Fields) }.

parse_enum(Enum) ->
    [Name, Symbols] = get_attributes(Enum, [<<"name">>, <<"symbols">>]),
    #avro_enum{ name    = binary_to_latin1_atom(Name), %% From Avro spec.: [A-Za-z0-9_]
		symbols = Symbols }.

parse_union(_Union) ->
    exit(not_implemented).

parse_map(Map) ->
    [ValuesType] = get_attributes(Map, [<<"values">>]),
    #avro_map{ values = parse_type(ValuesType) }.


parse_fixed(Fixed) ->
    [Name, Size] = get_attributes(Fixed, [<<"name">>, <<"size">>]),
    #avro_fixed{ name    = binary_to_latin1_atom(Name), %% From Avro spec.: [A-Za-z0-9_]
		 size = Size }.

parse_array(_Array) ->
    exit(not_implemented).
