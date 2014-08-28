PROJECT = eavro

REBAR = $(shell which rebar 2>/dev/null || echo $(PWD)/rebar)
DIALYZER = dialyzer
APACHE_DOWNLOAD_SITE = http://apache-mirror.rbc.ru/pub/apache/avro/avro-1.7.7/java

.PHONY: test deps

compile:
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean

test:
	@$(REBAR) xref eunit skip_deps=true

run: compile
	./start

avro_tools:
	mkdir avro_tools
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
