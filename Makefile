PROJECT = eavro

REBAR = $(shell which rebar 2>/dev/null || echo $(PWD)/rebar)
LIBS = ERL_LIBS=deps:../
DIALYZER = dialyzer
APACHE_DOWNLOAD_SITE = http://apache-mirror.rbc.ru/pub/apache/avro/avro-1.7.7/java

.PHONY: jtest test deps avro_tools

compile:
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean

jtest:
	(cd jtest && mvn compile assembly:single)
test: jtest
	@$(REBAR) xref eunit skip_deps=true

run: compile
	@$(LIBS) erl -pa ebin

run_srv: compile
	@$(LIBS) erl -s eavro_rpc_srv


avro_tools:
	@[ -d avro_tools ] || mkdir avro_tools
	(cd avro_tools && wget $(APACHE_DOWNLOAD_SITE)/avro-tools-1.7.7.jar && wget $(APACHE_DOWNLOAD_SITE)/avro-1.7.7.jar)
generate_avro_data: avro_tools
	java -jar avro_tools/avro-tools-1.7.7.jar fromjson test/data/transformers.json --schema-file test/data/transformer.avsc > test/data/transformers.avro

generate_avro_data_deflate: avro_tools
	 java -jar avro_tools/avro-tools-1.7.7.jar fromjson test/data/transformers.json --codec deflate --schema-file test/data/transformer.avsc > test/data/transformers-deflated.avro

build-plt:
	@$(DIALYZER) --build_plt --output_plt $(PROJECT).plt \
        --apps erts kernel stdlib compiler crypto

dialyze:
	@$(DIALYZER) --src src --plt $(PROJECT).plt \
        -Werror_handling -Wrace_conditions -Wunmatched_returns
