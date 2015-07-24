%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(replication_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    [Nodes] = rt:build_clusters([1]),

    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    Node = hd(Nodes),
    rt:wait_for_service(Node, antidote),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(Node,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    rt:log_to_nodes(Nodes, "Starting write operation 1"),


    IfRepl = rpc:call(Node, antidote_config, get, [do_repl]),
    case IfRepl of 
        true ->
            random:seed({1,2,3}),
            Keys = [ random:uniform(1000)  ||  _X <- lists:seq(1,20)],
            lists:foreach(fun(Key) -> 
                    {ok, _} = rpc:call(Node, antidote, append, [Key, riak_dt_pncounter, {increment, haha}])
                        end, Keys),
            
            lists:foreach(fun(Key) -> 
                        check_log_replicated(Node, Key)
                        end, Keys),
            pass;
        false ->
            pass
    end.

check_log_replicated(Node, Key) ->
    [{Part,_N}] = rpc:call(Node, log_utilities, get_preflist_from_key, [Key]),
    Successors = rpc:call(Node, log_utilities, get_my_next, [Part, 2]),
    MyOwnLog = rpc:call(Node, repl_fsm, retrieve_log, [Part,Part]),
    lager:info("My log is: ~w", [MyOwnLog]),
    lists:foreach(fun({Successor, _Node}) ->
                    SuccLog = rpc:call(Node, repl_fsm,retrieve_log,[Successor, Part]),
                    ?assertEqual(SuccLog, MyOwnLog) end,
                    Successors).

