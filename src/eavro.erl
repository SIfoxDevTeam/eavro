-module(eavro).

%% API
-export([read_ocf/1, read_ocf/2, read_schema/1, parse_schema/1]).
-export([encode/2]).

-include("eavro.hrl").

read_ocf(File) -> 
    read_ocf(File, undefined).

read_ocf(File, Hook) ->
    {ok, Bin} = file:read_file(File),
    eavro_ocf_codec:decode(Bin,Hook).

read_schema(File) ->
    case file:read_file(File) of
        {ok, Data} ->
            Schema = jsx:decode(Data),
            parse_schema(Schema);
        Error -> Error
    end.

parse_schema(SchemaJson) when is_binary(SchemaJson) ->
    parse_schema(jsx:decode(SchemaJson));
parse_schema(SchemaJsx) ->
    parse_type(SchemaJsx).


encode(Schema, Data) ->
    iolist_to_binary(eavro_codec:encode(Schema, Data)).

%%
%% Private functions section
%%

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
parse_type(_Bad) -> exit({badarg, _Bad}).


get_attributes(Complex, Attrs) ->
    [proplists:get_value(Attr, Complex) || Attr <- Attrs].

binary_to_latin1_atom(Bin) ->
    binary_to_atom(Bin,latin1).

parse_record(Record) ->
    [Name, Fields] = get_attributes(Record, [<<"name">>, <<"fields">>]),
    #avro_record{ name   = binary_to_latin1_atom(Name), %% From Avro spec.: [A-Za-z0-9_]
		  fields = lists:map(fun parse_field/1, Fields)}.

parse_field(RecField) -> 
    [Name, Type] = get_attributes(RecField, [<<"name">>, <<"type">>] ),
    {Name, parse_type(Type)}.

parse_enum(Enum) ->
    [Name, Symbols] = get_attributes(Enum, [<<"name">>, <<"symbols">>]),
    #avro_enum{ name    = binary_to_latin1_atom(Name), %% From Avro spec.: [A-Za-z0-9_]
		symbols = lists:map(fun binary_to_latin1_atom/1, Symbols) }.

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
