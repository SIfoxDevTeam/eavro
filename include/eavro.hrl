-record(avro_record, {
    name :: atom(),
    fields :: [{atom(), avro_type()}]
}).

-record(avro_enum,  { name :: atom(), symbols :: [ atom() ] }).

-record(avro_fixed, { name :: atom(), size :: integer() }).

-record(avro_map,   { values :: avro_type() }).

-record(avro_array, { items :: avro_type() }).

-type avro_type() :: #avro_record{} |
		     #avro_enum{}   |
                      int           | 
                      long          | 
                      double        |
                      bytes         |
                      boolean       |
                      string        | 
                      null .
-type decode_hook() :: fun( (avro_type(), any() )  -> any() ).
