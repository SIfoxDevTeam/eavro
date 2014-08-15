-module(eavro).

%% API
-export([schema/1]).
-export([encode/2]).
-export([format_error/1]).

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

-spec format_error(atom()) -> string().
format_error(enotype) ->
    "No type of schema is specified";
format_error(ebadtype) ->
    "Unsupported type of schema is specified".

%% Internal functions

data_type(<<"null">>)   -> null;
data_type(<<"boolean">>)-> boolean;
data_type(<<"int">>)    -> int;
data_type(<<"long">>)   -> long;
data_type(<<"double">>) -> double;
data_type(<<"string">>) -> string;
data_type(<<"binary">>) -> string;
data_type(_Other)       ->
    {error, ebadtype}.

parse_schema(Schema) ->
    case lists:keyfind(<<"type">>, 1, Schema) of
        {<<"type">>, <<"record">>} ->
            parse_record(Schema);
        {<<"type">>, Type} ->
            data_type(Type);
        false ->
            {error, enotype}
    end.

parse_record(Schema) ->
    Name = proplists:get_value(<<"name">>, Schema),
    Fields = proplists:get_value(<<"fields">>, Schema),
    #avro_record{
        name = Name,
        fields = [parse_record_field(Field) || Field <- Fields]
    }.

parse_record_field(Field) ->
    Name = proplists:get_value(<<"name">>, Field),
    Type = proplists:get_value(<<"type">>, Field),
    {Name, data_type(Type)}.
