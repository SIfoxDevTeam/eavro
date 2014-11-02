-module(eavro).

%% API exports
-export([read_ocf/1, 
	 read_ocf/2,
	 read_ocf_with/2,
	 read_ocf_with/3,
	 read_schema/1,
	 write_ocf/3,
	 write_ocf/4,
	 parse_schema/1,
	 parse_type/2,
	 parse_types/2,
	 encode_schema/1,
	 encode/2, 
	 decode/2, 
	 decode/3]).

-export([ type_to_jsx/1 ]).

-include("eavro.hrl").

%%=======================
%% API functions
%%======================

-type type_context() :: dict:dict(atom(), avro_type()).

%%
%%
%%
-spec read_ocf(Filename :: file:filename()) -> 
		      {Schema :: avro_type(), 
		       Blocks :: [ [ any() ] ]}.
read_ocf(File) -> 
    read_ocf(File, undefined).

%%
%%
%%
-spec read_ocf(File :: file:filename(),
	       Hook :: undefined | decode_hook()) -> 
		      {Schema :: avro_type(), 
		       Blocks :: [ [ any() ] ]}.
read_ocf(File, Hook) ->
    {ok, Bin} = file:read_file(File),
    eavro_ocf_codec:decode(Bin,Hook).

%%
%%
%%
-spec read_schema(File :: file:filename() ) -> avro_type().
read_schema(File) ->
    case file:read_file(File) of
        {ok, Data} ->
            Schema = jsx:decode(Data),
            parse_schema(Schema);
        Error -> Error
    end.

%%
%% Read OCF using callback function which accept two 
%% arguments - schema and Z-List of instances. Function 
%% returns a result of callback.
%%
-spec read_ocf_with(
	File    :: file:filename(),
	Visitor :: eavro_ocf_zcodec:ocf_visitor(Result) ) -> Result.
read_ocf_with(File, Visitor) ->
    read_ocf_with(File, Visitor, undefined).

%%
%% Read OCF using callback function which accept two 
%% arguments - schema and Z-List of instances, and 
%% decode hook callback to transform instances just 
%% when they decoded in a default way. Function 
%% returns a result of callback.
%%
-spec read_ocf_with(
	File    :: file:filename(),
	Visitor :: eavro_ocf_zcodec:ocf_visitor(Result), 
	Hook    :: decode_hook() ) -> Result.
read_ocf_with(File, Visitor, Hook) ->
    eavro_ocf_zcodec:read_ocf_with(File, Visitor, Hook).

%%
%% Write OCF with given schema and instances.
%%
-spec write_ocf(
	Filename   :: file:filename(), 
	Schema     :: avro_type(), 
	ZInstances :: zlists:zlist()) -> ok.
write_ocf(Filename, Schema, ZInstances) ->
    write_ocf(Filename, Schema, ZInstances, []).

%%
%% Write OCF with given schema, instances, and options which 
%% controll binary format details such as compression codec 
%% used (currently only 'deflate' and 'plain' supported), and 
%% size of block. If size of block is specified it does not 
%% mean that block will have strictly that size, this value 
%% just mean a threshold of written bytes into block which is 
%% when exceeded then a new block started.
%%
-spec write_ocf(
	Filename   :: file:filename(), 
	Schema     :: avro_type(), 
	ZInstances :: zlists:zlist(),
	Opts :: [{codec, deflate | plain} |
		 {block_size, non_neg_integer()}]) -> ok.
write_ocf(Filename, Schema, ZInstances, Opts) ->
    eavro_ocf_zcodec:write_ocf_file(Filename, Schema, ZInstances, Opts).

%%
%% Parse JSONed schema.
%%
-spec parse_schema( binary() ) -> avro_type().
parse_schema(SchemaJson) when is_binary(SchemaJson) ->
    parse_schema(jsx:decode(SchemaJson));
parse_schema(SchemaJsx) ->
    {Type, _Ctx} = parse_type(SchemaJsx, dict:new()),
    Type.

%%
%% Encode schema as JSON.
%%
-spec encode_schema(Schema :: avro_type()) -> binary().
encode_schema(Schema) ->
    jsx:encode(type_to_jsx(Schema)).

%%
%%
%%
decode(Schema, Buff) -> 
    decode(Schema, Buff, undefined).

%%
%%
%%
-spec decode( Schema :: avro_type(), 
              Buff :: binary() | iolist(), 
              Hook :: undefined | decode_hook() ) -> 
    { Value :: term(), Buff :: binary()}.
decode(Schema, Buff, Hook) ->
    eavro_codec:decode(Schema, Buff, Hook).
%%
%%
%%
encode(Schema, Data) ->
    iolist_to_binary(eavro_codec:encode(Schema, Data)).

%%
%% Private functions section
%%

type_to_jsx(#avro_record{ name = Name, fields = Fields}) ->
    [{type, <<"record">>},
     {name, to_bin(Name)}, 
     {fields, [ [ {name, to_bin(FName)},
		  {type, type_to_jsx(FType)} ] || {FName, FType} <- Fields]} ];
type_to_jsx(#avro_enum{ name = Name, symbols = Symbols}) ->
    [{type, <<"enum">>},
     {name, to_bin(Name)},
     {symbols, [ to_bin(Symbol) || Symbol <- Symbols]} ];
type_to_jsx(#avro_fixed{ name = Name, size = Size }) ->
    [{type, <<"fixed">>},{name, to_bin(Name)}, {size, Size}];
type_to_jsx(#avro_map{ values = VType}) ->
    [{type, <<"map">>},
     {values, type_to_jsx(VType)}];
type_to_jsx(#avro_array{ items = IType}) ->
    [{type, <<"array">>},
     {items, type_to_jsx(IType)}];
type_to_jsx(Union) when is_atom(hd(Union)) -> 
    [ type_to_jsx(T) || T <- Union];
type_to_jsx(A) when is_atom(A) ->
    type_to_jsx(atom_to_binary(A,latin1));
type_to_jsx(B) when is_binary(B) ->
    case B of
	<<"null">>    -> ok;
	<<"boolean">> -> ok;
	<<"int">>     -> ok;
	<<"long">>    -> ok;
	<<"double">>  -> ok;
	<<"string">>  -> ok;
	<<"bytes">>   -> ok;
	<<"float">>   -> ok;
	BadType       -> exit({bad_simple_type, BadType})
    end,
    B.

parse_types(Types, Context) ->    
    {TypesRev, Context1} = 
	lists:foldl(
	  fun(Type, {Ts, Ctx}) ->
		  {T, Ctx1} = parse_type(Type, Ctx),
		  {[T|Ts], Ctx1}
	  end, {[], Context}, Types),
    {lists:reverse(TypesRev), Context1}.

-spec parse_type(Jsx :: jsx:json_term(), 
		 Context :: type_context()) ->
			{avro_type(), type_context() }.
parse_type(Simple, Context) when is_binary(Simple) ->
    Type = 
	case Simple of
	    <<"null">>    -> null;
	    <<"boolean">> -> boolean;
	    <<"int">>     -> int;
	    <<"long">>    -> long;
	    <<"double">>  -> double;
	    <<"string">>  -> string;
	    <<"bytes">>   -> bytes;
	    <<"float">>   -> float;
	    BadType       -> 
		case dict:find(binary_to_atom(BadType, latin1), Context) of
		    {ok, T} -> T;
		    error   ->
			exit({bad_simple_type_or_alias, BadType, Context})
		end
	end,
    {Type, Context};
parse_type([{_,_}|_] = Complex, Context) ->
    Parser = 
	case proplists:get_value(<<"type">>,Complex) of
	    <<"record">> ->
		fun parse_record/2;
	    <<"enum">> ->
		fun parse_enum/2;
	    <<"map">> ->
		fun parse_map/2;
	    <<"array">> ->
		fun parse_array/2;
	    <<"fixed">> ->
		fun parse_fixed/2;
	    BadType -> exit({bad_complex_type, BadType})
	end,
    Parser(Complex, Context);
parse_type([B|_] = Union, Context) when is_binary(B) -> 
    parse_union(Union, Context);
parse_type(_Bad,_) -> exit({badarg, _Bad}).


get_attributes(Complex, Attrs) ->
    [proplists:get_value(Attr, Complex) || Attr <- Attrs].

binary_to_latin1_atom(Bin) ->
    binary_to_atom(Bin,latin1).

parse_record(Record, Context) ->
    [Name, Fields] = get_attributes(Record, [<<"name">>, <<"fields">>]),
    {FieldsParsedRev, Context1} = 
	lists:foldl(
	  fun(Field, {Fs, Ctx})-> 
		  {FieldParsed, Ctx1} = parse_field(Field, Ctx),
		  {[FieldParsed | Fs], Ctx1}
	  end, {[],Context}, Fields),
    FieldsParsed = lists:reverse(FieldsParsedRev),
    AName = binary_to_latin1_atom(Name), %% From Avro spec.: [A-Za-z0-9_]
    RecTypeParsed = 
	#avro_record{ name   = AName,
		      fields = FieldsParsed},
    {RecTypeParsed, 
     dict:store(AName, RecTypeParsed, Context1)}.

parse_field(RecField, Context) -> 
    [Name, Type] = get_attributes(RecField, [<<"name">>, <<"type">>] ),
    {TypeParsed, Context1} = parse_type(Type, Context),
    {{Name, TypeParsed}, Context1}.

parse_enum(Enum, Context) ->
    [Name, Symbols] = get_attributes(Enum, [<<"name">>, <<"symbols">>]),
    AName = binary_to_latin1_atom(Name), %% From Avro spec.: [A-Za-z0-9_]
    TypeParsed = #avro_enum{ name    = AName,
		 symbols = lists:map(fun binary_to_latin1_atom/1, Symbols) },
    {TypeParsed,
     dict:store(AName,TypeParsed,Context)}.

parse_union(Union,Context) ->
    {Types, Context1} = parse_types(Union, Context),
    check_uniqueness(Types),
    {Types, Context1}.

check_uniqueness(Types) ->
    L0 = [case T of
	 #avro_record{ name = N } -> N;
	 #avro_enum{   name = N } -> N;
	 #avro_fixed{  name = N } -> N;
	 _ -> T
     end || T <- Types],
    L = lists:zip(L0, lists:seq(0, length(L0) - 1)),
    [ if N1 == N2 -> exit({bad_union, Types, {name_clash, {Idx1, T1}, {Idx2,T2} } });
	 true -> ok 
      end|| {{T1, N1}, Idx1} <- L, {{T2,N2}, Idx2} <- L, Idx1 < Idx2],
    ok.
	 
	 

parse_map(Map, Context) ->
    [ValuesType] = get_attributes(Map, [<<"values">>]),
    {ValuesTypeParsed, Context1} = parse_type(ValuesType, Context),
    {#avro_map{ values = ValuesTypeParsed }, Context1}.

parse_fixed(Fixed,Context) ->
    [Name, Size] = get_attributes(Fixed, [<<"name">>, <<"size">>]),
    AName = binary_to_latin1_atom(Name), %% From Avro spec.: [A-Za-z0-9_]
    Type = #avro_fixed{ name    = AName,
			size = Size },
    {Type, dict:store(AName, Type, Context)}.

parse_array(Array, Context) ->
    [Type] = get_attributes(Array, [<<"items">>]),
    {ParsedType, Context1} = parse_type(Type, Context),
    {#avro_array{ items = ParsedType }, Context1}.


to_bin(B) when is_binary(B) ->
    B;
to_bin(A) when is_atom(A) ->
    atom_to_binary(A,latin1);
to_bin(L) when is_list(L) ->
    list_to_binary(L).
