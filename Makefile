.PHONY: deps compile test

REBAR := ./rebar
all: gen_nfs
	@$(REBAR) compile
	#@$(REBAR) xref skip_deps=true
	@$(REBAR) eunit skip_deps=true
deps:
	@$(REBAR) get-deps
compile:
	@$(REBAR) compile skip_deps=true
xref:
	@$(REBAR) xref skip_deps=true
eunit:
	@$(REBAR) eunit skip_deps=true
clean:
	@$(REBAR) clean skip_deps=true
distclean:
	@$(REBAR) delete-deps
	@$(REBAR) clean
qc:
	@$(REBAR) qc skip_deps=true
gen_rpc: deps
	(cd deps/erpcgen/;make rpc)
gen_nfs: gen_rpc
	./deps/erpcgen/priv/erpcgen -a [svc_callback,xdr,hrl] src/leo_gateway_nfs_proto3.x
	./deps/erpcgen/priv/erpcgen -a [svc_callback,xdr,hrl] src/leo_gateway_nfs_mount3.x
