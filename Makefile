REBAR		?= ./rebar

all: compile
	make -C deps/basho_bench all
	cp deps/basho_bench/basho_bench .

deps:
	$(REBAR) get-deps

compile: deps
	$(REBAR) compile

clean:
	$(REBAR) clean
	rm -f ./basho_bench

results:
	./deps/basho_bench/priv/summary.r -i tests/current

.PHONY: all deps compile clean results
