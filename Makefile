REBAR = ./rebar

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean

test:
	@$(REBAR) eunit
