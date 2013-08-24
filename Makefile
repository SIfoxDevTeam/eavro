REBAR = ./rebar

compile:
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean

test:
	@$(REBAR) eunit
