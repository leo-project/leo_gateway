.PHONY: deps compile test

REBAR := ./rebar
APPS = erts kernel stdlib sasl crypto compiler inets mnesia public_key runtime_tools snmp syntax_tools tools xmerl webtool
LIBS = deps/leo_commons/ebin deps/leo_logger/ebin deps/leo_object_storage/ebin \
       deps/leo_redundant_manager/ebin deps/leo_mq/ebin deps/leo_statistics/ebin deps/leo_rpc/ebin \
       deps/leo_s3_libs/ebin deps/leo_cache/ebin deps/leo_dcerl/ebin deps/leo_mcerl/ebin \
       deps/savanna_commons/ebin deps/savanna_agent/ebin deps/erpcgen/ebin
PLT_FILE = .leo_gateway_dialyzer_plt
DOT_FILE = leo_gateway.dot
CALL_GRAPH_FILE = leo_gateway.png

all: deps gen_nfs
	@$(REBAR) compile
	@$(REBAR) xref skip_deps=true
	@$(REBAR) eunit skip_deps=true
deps:
	@$(REBAR) get-deps
compile:
	@$(REBAR) compile skip_deps=true
xref:
	@$(REBAR) xref skip_deps=true
eunit:
	@$(REBAR) eunit skip_deps=true
check_plt:
	@$(REBAR) compile
	dialyzer --check_plt --plt $(PLT_FILE) --apps $(APPS)
build_plt:
	@$(REBAR) compile
	dialyzer --build_plt --output_plt $(PLT_FILE) --apps $(APPS) $(LIBS)
dialyzer:
	@$(REBAR) compile
	dialyzer --plt $(PLT_FILE) -r ebin/ --dump_callgraph $(DOT_FILE) -Wrace_conditions | fgrep -v -f ./dialyzer.ignore-warnings
typer:
	typer --plt $(PLT_FILE) -I include/ -r src/
doc: compile
	@$(REBAR) doc
callgraph: graphviz
	dot -Tpng -o$(CALL_GRAPH_FILE) $(DOT_FILE)
graphviz:
	$(if $(shell which dot),,$(error "To make the depgraph, you need graphviz installed"))
clean:
	@$(REBAR) clean skip_deps=true
distclean:
	@$(REBAR) delete-deps
	@$(REBAR) clean
qc:
	@$(REBAR) qc skip_deps=true
gen_rpc: deps
	(cd deps/erpcgen/;make)
gen_nfs: gen_rpc
	./deps/erpcgen/priv/erpcgen -a [svc_callback,xdr,hrl] src/leo_nfs_proto3.x
	./deps/erpcgen/priv/erpcgen -a [svc_callback,xdr,hrl] src/leo_nfs_mount3.x
