-module(eavro_ocf_zcodec).

-include("eavro.hrl").

-export( [ read_ocf_with/2, 
	   read_ocf/1,
	   read_ocf_with/3, 
	   read_ocf/2,
	   write_ocf/4,
	   write_ocf/3,
	   write_ocf_file/4,
	   write_ocf_file/3,
	   read_ocf_blocks/1] ).

-export_type([ocf_visitor/1]).

-record(ocf_header,
	{ schema, 
	  compression_codec, 
	  sync }).

-type ocf_visitor(T) :: fun ( (Schema         :: avro_type(), 
			       ZInstancesRead :: zlists:zlist(term()) ) -> T ).

-define(EXPAND(Tail), if is_function(Tail, 0) -> Tail();
			 is_list(Tail) -> Tail end).

-define(ECHO(T), io:format("~p| ~p~n", [?LINE, T])).
%%=========================================================
%% API
%%=========================================================

write_ocf_file(Filename, Schema, ZInstances) ->
    write_ocf_file(Filename, Schema, ZInstances, []).

write_ocf_file(Filename, Schema, ZInstances, Opts) ->
    FOpts = proplists:get_value(file_opts, Opts, [raw]),
    {ok, Io} = file:open(Filename, [write | FOpts]),
    try write_ocf(Io, Schema, ZInstances, Opts)
    after
	file:close(Io)
    end.

write_ocf(Io, Schema, ZInstances) ->
    write_ocf(Io, Schema, ZInstances, []).

write_ocf(Io, Schema, ZInstances, Opts) ->
    Codec0    = proplists:get_value(codec, Opts, plain), 
    BlockSize = proplists:get_value(block_size, Opts, 1024*1024*10),
    Codec = to_bin(Codec0),
    Sync  = crypto:rand_bytes(16),
    PrivDir = code:priv_dir(eavro),
    OcfSchema = eavro:read_schema(filename:join(PrivDir, "avro_ocf.avsc")),
    Meta      = [{<<"avro.schema">>, eavro:encode_schema(Schema)}] ++
		 case Codec0 of 
		     plain -> []; 
		     _ -> [{<<"avro.codec">>, Codec}]
		     end,
    Z = open_codec(Codec),
    AddInstanceToBlock = 
	fun(Instance, {BlockInstCount, CurrBlockSize, BlockBytes}) ->
		EncBytes = eavro_codec:encode(Schema, Instance),
		CompressedBytes = compress(Z, EncBytes),
		{BlockInstCount + 1, 
		 CurrBlockSize + iolist_size(CompressedBytes), 
		 [CompressedBytes | BlockBytes]}
	end,
    FlushBlock = 
	fun({BlockInstCount, CurrBlockSize, BlockBytes}) ->
		Tail = end_compress(Z),
		ok = file:write(Io,eavro_codec:encode(long,  BlockInstCount)),
		ok = file:write(Io,eavro_codec:encode(
				long, CurrBlockSize + iolist_size(Tail))),
		ok = file:write(Io,lists:reverse([Tail|BlockBytes])),
		ok = file:write(Io,Sync)
	end,
    try ok = file:write(
	       Io, 
	       eavro_codec:encode(OcfSchema, [<<"Obj",1>>, Meta, Sync]) ),
	 init_codec(Z),
	 LastBlock = 
	     zlists:foldl(
	       fun
		   (Instance, {_BlockInstCount, CurrBlockSize, _BlockBytes} = Block) 
		     when CurrBlockSize >= BlockSize ->
		       FlushBlock(Block),
		       init_codec(Z),
		       AddInstanceToBlock(Instance, {0,0,[]});
		   (Instance, Block) ->
		       AddInstanceToBlock(Instance, Block)
	       end, {0,0,[]}, ZInstances),
	 FlushBlock(LastBlock),
	 ok
    after
	close_codec(Z)
    end.

init_codec(<<"plain">>) -> ok;
init_codec(plain) -> ok;
init_codec({<<"deflate">>, Z}) ->
    zlib:deflateInit(Z, default, deflated, -15, 9, default).

end_compress(<<"plain">>) -> <<>>;
end_compress(plain) -> <<>>;
end_compress({<<"deflate">>, Z}) -> 
    B = zlib:deflate(Z,<< >>,finish),
    zlib:deflateEnd(Z),
    B.
    

compress(<<"plain">>, B) -> B;
compress(plain, B) -> B;
compress({<<"deflate">>, Z}, B) ->
    zlib:deflate(Z, B, sync).




to_bin(B) when is_binary(B) ->
    B;
to_bin(A) when is_atom(A) ->
    atom_to_binary(A,latin1);
to_bin(L) when is_list(L) ->
    list_to_binary(L).

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
    read_ocf_with(File, Visitor, undefined).

-spec read_ocf_with(
	File    :: file:filename(),
	Visitor :: ocf_visitor(Result), 
	Hook    :: decode_hook() ) -> Result.
read_ocf_with(File, Visitor, Hook) ->
    {ok, Io} = file:open(File, [read, raw, binary]),
    try {Schema, ZInstances, CodecFinalizer} = read_ocf(Io, Hook),
	 try Visitor(Schema, ZInstances)
	 after
	     CodecFinalizer()
	 end
    after
	file:close(Io)
    end.

read_ocf_blocks(Filename) ->
    {ok, Io} = file:open(Filename, [read, raw, binary]),
    ZBytes = zlists_file:read(Io, 1024*64),
    { Header = #ocf_header{}, ZBytes1 } = read_ocf_header(ZBytes),
    %%
    ReadBlock = 
	fun
	    ([], _Cont) -> [];
	    (ZBuff, Cont) ->
		{Count, ZBuff1} = eavro_zcodec:decode(long, ZBuff),
		{BSize, ZBuff2} = eavro_zcodec:decode(long, ZBuff1),
		[<<BlockBytes:BSize/binary, Sync1:16/binary>> | ZBuff3] = 
		    zlists_file:expand_binary(ZBuff2, BSize + 16),
		[ {Count, BSize, BlockBytes, Sync1} | 
		  fun() -> Cont(?EXPAND(ZBuff3), Cont) end ]
	end,
    { Header, ReadBlock(ZBytes1, ReadBlock) }.

%%
%% Reads and decodes Avro OCF file and returns a schema and instances as a Z-List.
%%
-spec read_ocf(Io :: file:io_device() ) -> 
		      { avro_type(), zlists:zlist( term() ), fun( () -> ok ) }.
read_ocf(Io) ->
    read_ocf(Io, undefined).

-spec read_ocf(Io   :: file:io_device(), 
	       Hook :: decode_hook() ) -> 
		      { avro_type(), zlists:zlist( term() ), fun( () -> ok ) }.
read_ocf(Io, Hook) ->
    { Schema, ZBytes, CodecFinalizer} = read_ocf_(Io),
    ZInstances = eavro_zcodec:decode_seq(
		   Schema, Hook, fun eavro_zcodec:decode/3, ZBytes),
    { Schema, ZInstances, CodecFinalizer}.    
%%=========================================================
%% Private area
%%=========================================================

read_ocf_header(ZBytes) ->
    %% Read header
    PrivDir = code:priv_dir(eavro),
    OcfSchema = eavro:read_schema(filename:join(PrivDir, "avro_ocf.avsc")),
    {_Header = [_,Meta,Sync], ZBytes1} = eavro_zcodec:decode(OcfSchema, ZBytes),
    SchemaJson = proplists:get_value(<<"avro.schema">>, lists:flatten(Meta)),
    Codec  = proplists:get_value(<<"avro.codec">>, lists:flatten(Meta)),
    Schema = eavro:parse_schema(SchemaJson),
    { #ocf_header{ 
	 schema = Schema, 
	 compression_codec = Codec, 
	 sync = Sync}, ZBytes1 }.

read_ocf_(Io) ->
    ZBytes = zlists_file:read(Io, 1024*64),
    { #ocf_header{ 
	 schema = Schema, 
	 compression_codec = Codec, 
	 sync = _Sync}, ZBytes1 } = read_ocf_header(ZBytes),
    %%
    CodecSession = open_codec(Codec),
    try
	{Schema, 
	 read_ocf_blocks_continuosly(CodecSession, ZBytes1), 
	 fun()-> close_codec(CodecSession) end}
    catch
	_:Reason ->
	    exit({failed_read_ocf_blocks, Reason}),
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


read_ocf_blocks_continuosly(_Codec, []) -> [];
read_ocf_blocks_continuosly(_Codec, [<<>>]) -> [];
read_ocf_blocks_continuosly(Codec, ZBytes) ->
    {_ObjCount, ZBytes1} = eavro_zcodec:decode(long, ZBytes),
    {BlockSize, ZBytes2} = eavro_zcodec:decode(long, ZBytes1),
    uncompress_ocf_block(Codec, ZBytes2, BlockSize).

uncompress_ocf_block({<<"deflate">>, Z}, ZBuff, BlockSize) ->
    ok = zlib:inflateInit(Z,-15),
    inflate_ocf_block(Z, ZBuff, BlockSize);
uncompress_ocf_block(<<"snappy">>, _ZBuff, _BlockSize) ->
    exit(unsupported);
uncompress_ocf_block(Plain, ZBuff, BlockSize) 
  when Plain == plain;Plain== <<"plain">> ->
    plain_ocf_block(ZBuff, BlockSize).

plain_ocf_block(ZBuff, 0) when is_list(ZBuff) -> 
    [<<_Sync:16/binary>> | ZBuff1] = zlists_file:expand_binary(ZBuff, 16),
    read_ocf_blocks_continuosly(plain, ?EXPAND(ZBuff1));
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
    read_ocf_blocks_continuosly({<<"deflate">>, Z}, ?EXPAND(ZBuff1));
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
