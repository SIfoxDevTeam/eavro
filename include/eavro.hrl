-record(avro_record, {
    name :: binary(),
    fields :: [{atom(), avro_type()}]
}).

-record(avro_enum, { name :: atom(), symbols :: [ atom() ] }).

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
