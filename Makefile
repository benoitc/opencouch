ERL ?= erl
ERLC ?= erlc
REBAR ?= rebar

all: deps compile

compile:
	@$(REBAR) compile

deps:
	@$(REBAR) get-deps

clean:
	@$(REBAR) clean
	@rm -f t/*.beam

test:
	@$(ERLC) -o t/ t/etap.erl
	prove t/*.t

verbose-test:
	@$(ERLC) -o t/ t/etap.erl
	prove -v t/*.t

cover: test
	@ERL_FLAGS="-pa ./ebin -pa ./t" \
		$(ERL) -detached -noshell -eval 'etap_report:create()' -s init stop
