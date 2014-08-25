-module(eavro_ocf_zcodec).

-include("eavro.hrl").

-export( [ read_ocf_with/2, 
	   read_ocf/1 ] ).

-export_type([ocf_visitor/1]).

-type ocf_visitor(T) :: fun ( (Schema         :: avro_type(), 
			       ZInstancesRead :: zlists:zlist(term()) ) -> T ).

-define(EXPAND(Tail), if is_function(Tail, 0) -> Tail();
			 is_list(Tail) -> Tail end).

%%=========================================================
%% API
%%=========================================================

%%
%% Opens an Avro OCF file, reads schema and instances and passes 
%% them into callback function. This function helps to make reading 
%% more safe due to it handles properly closing of a file and a zlib 
%% stream (when 'deflate' codec used).
%% 
%% Also this function gives a possibility to read huge OCF files in a 
%% lazy, memory effecient way. Instances passed into callback function 
%% as a lazy list, i.e. improper list in a form [H1,...,Hn | Fun], where 
%% Fun - is a fun( () -> [H1',...,Hn' | Fun1]) etc.. Such an improper 
%% list may be consumed directly or by means of 'zlists' library 
%% (See github for erlang-zlists).
%%
-spec read_ocf_with(
	File    :: file:filename(),
	Visitor :: ocf_visitor(Result) ) -> Result.
read_ocf_with(File, Visitor) ->
    {ok, Io} = file:open(File, [read, raw, binary]),
    try {Schema, ZInstances, CodecFinalizer} = read_ocf(Io),
	 try Visitor(Schema, ZInstances)
	 after
	     CodecFinalizer()
	 end
    after
	file:close(Io)
    end.

%%
%% Reads and decodes Avro OCF file and returns a schema and instances as a Z-List.
%%
-spec read_ocf(Io :: file:io_device() ) -> 
		      { avro_type(), zlists:zlist( term() ), fun( () -> ok ) }.
read_ocf(Io) ->
    { Schema, ZBytes, CodecFinalizer} = read_ocf_(Io),
    ZInstances = eavro_zcodec:decode_seq(
		   Schema, undefined, fun eavro_zcodec:decode/3, ZBytes),
    { Schema, ZInstances, CodecFinalizer}.
    
%%=========================================================
%% Private area
%%=========================================================
read_ocf_(Io) ->
    %% Read header
    PrivDir = code:priv_dir(eavro),
    OcfSchema = eavro:read_schema(filename:join(PrivDir, "avro_ocf.avsc")),
    ZBytes = zlists_file:read(Io, 32),
    {_Header = [_,Meta,_], ZBytes1} = eavro_zcodec:decode(OcfSchema, ZBytes),
    SchemaJson = proplists:get_value(<<"avro.schema">>, lists:flatten(Meta)),
    Codec  = proplists:get_value(<<"avro.codec">>, lists:flatten(Meta)),
    Schema = eavro:parse_schema(SchemaJson),
    %%
    CodecSession = open_codec(Codec),
    try
	{Schema, 
	 read_ocf_blocks(CodecSession, ZBytes1), 
	 fun()-> close_codec(CodecSession) end}
    catch
	_:_ ->
	    close_codec(CodecSession)
    end.

open_codec(<<"deflate">> = Codec) ->
    {Codec, zlib:open()};
open_codec(<<"snappy">>) ->
    exit(unsupported);
open_codec(_) ->
    plain.

close_codec({<<"deflate">>, Z}) ->
    zlib:close(Z), ok;
close_codec(_) ->
    ok.


read_ocf_blocks(_Codec, []) -> [];
read_ocf_blocks(_Codec, [<<>>]) -> [];
read_ocf_blocks(Codec, ZBytes) ->
    {_ObjCount, ZBytes1} = eavro_zcodec:decode(long, ZBytes),
    {BlockSize, ZBytes2} = eavro_zcodec:decode(long, ZBytes1),
    uncompress_ocf_block(Codec, ZBytes2, BlockSize).

uncompress_ocf_block({<<"deflate">>, Z}, ZBuff, BlockSize) ->
    ok = zlib:inflateInit(Z,-15),
    inflate_ocf_block(Z, ZBuff, BlockSize);
uncompress_ocf_block(<<"snappy">>, _ZBuff, _BlockSize) ->
    exit(unsupported);
uncompress_ocf_block(_, ZBuff, BlockSize) ->
    plain_ocf_block(ZBuff, BlockSize).

plain_ocf_block(ZBuff, 0) when is_list(ZBuff) -> 
    [<<_Sync:16/binary>> | ZBuff1] = zlists_file:expand_binary(ZBuff, 16),
    read_ocf_blocks(plain, ?EXPAND(ZBuff1));
plain_ocf_block([Chunk | Tail], Size) ->
    ChunkSize = byte_size(Chunk),
    if ChunkSize =< Size ->
	    [Chunk | fun() -> plain_ocf_block(?EXPAND(Tail), Size - ChunkSize) end];
       ChunkSize > Size ->
	    <<LChunk:Size/binary, RChunk/binary>> = Chunk,
	    [LChunk | fun() -> plain_ocf_block([ RChunk | Tail ], 0) end]
    end.

inflate_ocf_block(Z, ZBuff, 0) ->
    ok = zlib:inflateEnd(Z),
    [<<_Sync:16/binary>> | ZBuff1] = zlists_file:expand_binary(ZBuff, 16),
    read_ocf_blocks(<<"deflate">>, ?EXPAND(ZBuff1));
inflate_ocf_block(Z,[Chunk | Tail], Size) ->
    ChunkSize = byte_size(Chunk),
    if ChunkSize =< Size ->
	    [iolist_to_binary(zlib:inflate(Z,Chunk)) | 
	     fun() -> 
		     inflate_ocf_block(Z,?EXPAND(Tail), Size - ChunkSize) 
	     end];
       ChunkSize > Size ->
	    <<LChunk:Size/binary, RChunk/binary>> = Chunk,
	    [iolist_to_binary(zlib:inflate(Z,LChunk)) | 
	     fun() -> inflate_ocf_block(Z,[ RChunk | Tail ], 0) end]
    end.
