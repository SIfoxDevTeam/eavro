REBAR = ./rebar

.PHONY: test

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
