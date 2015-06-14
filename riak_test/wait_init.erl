-module(wait_init).

-export([wait_ready_nodes/1,
	 check_ready/1]).


wait_ready_nodes([]) ->
    true;
wait_ready_nodes([Node|Rest]) ->
    case check_ready(Node) of
	true ->
	    wait_ready_nodes(Rest);
	false ->
	    false
    end.

check_ready(Node) ->
    lager:info("Waiting for clocksi"),
    case rpc:call(Node,clocksi_vnode,check_tables_ready,[]) of
	true ->
        lager:info("Waiting for readitemfsm"),
	    case rpc:call(Node,clocksi_readitem_fsm,check_servers_ready,[]) of
		true ->
                lager:info("Done"),
		    %case rpc:call(Node,materializer_vnode,check_tables_ready,[]) of
			%true ->
			    true;
			%false ->
			%    false
		    %end;
		false ->
		    false
	    end;
	false ->
	    false
    end.

    
