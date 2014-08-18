-module(eavro_ocf_codec).

-export([decode/1]).

%% See: http://avro.apache.org/docs/1.7.7/spec.html#Object+Container+Files

decode(Bin) ->
    PrivDir = code:priv_dir(eavro),
    OcfSchema = eavro:read_schema(filename:join(PrivDir, "avro_ocf.avsc")),
    {_Header = [_,Meta,_], Bin1} = eavro_codec:decode(OcfSchema,Bin),
    SchemaJson = proplists:get_value(<<"avro.schema">>, lists:flatten(Meta)),
    Codec = proplists:get_value(<<"avro.codec">>, lists:flatten(Meta)),
    Schema = eavro:parse_schema(SchemaJson),
    decode_blocks(Schema, Bin1, Codec).

decode_blocks(_Schema, <<>>, Codec) -> [];
decode_blocks(Schema, Buff, Codec) ->
    {ObjCount, Buff1}  = eavro_codec:decode(long, Buff),
    {BlockSize, Buff2} = eavro_codec:decode(long, Buff1),
    <<Block:BlockSize/binary, _Sync:16/binary, Buff3/binary>> = Buff2,
    Objs = decodeN(Schema, ObjCount, inflate(Block, Codec) ),
    [Objs | decode_blocks(Schema, Buff3, Codec)].

decodeN(_Schema, 0, <<>>) ->
    [];
decodeN(Schema, ObjCount, Buff) ->
    {Obj, Buff1} = eavro_codec:decode(Schema, Buff),
    [Obj | decodeN(Schema, ObjCount-1, Buff1)].

inflate(Block, _Codec) ->
    Block.
