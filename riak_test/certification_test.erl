%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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
-module(certification_test).

-export([confirm/0, 
         clocksi_test_certification_check/1,
         clocksi_pending_prepare_abort_check/1,
         spawn_prepare/3,
         clocksi_multiple_test_certification_check/1]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, 8}]}
    ]),
    [Nodes] = rt:build_clusters([3]),
    lager:info("Waiting for ring to converge."),
    rt:wait_until_ring_converged(Nodes),

    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(hd(Nodes),fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    lager:info("Nodes: ~p", [Nodes]),

    clocksi_test_certification_check(Nodes),
    clocksi_pending_prepare_abort_check(Nodes),
    clocksi_multiple_test_certification_check(Nodes),
    rt:clean_cluster(Nodes),
    pass.




%% @doc The following function tests the certification check algorithm,
%%      when two concurrent txs modify a single object, one hast to abort.
clocksi_test_certification_check(Nodes) ->
    lager:info("clockSI_test_certification_check started"),
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    %% Start a new tx,  perform an update over key write.
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, write, Type, {increment, 1}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),

    %% Start a new tx,  perform an update over key write.
    {ok,TxId1}=rpc:call(LastNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1=rpc:call(LastNode, antidote, clocksi_iupdate,
                          [TxId1, write, Type, {increment, 2}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),
    lager:info("Tx1 finished concurrent write..."),

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),

    %% commit the first tx.
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({aborted, TxId}, CommitTime),
    lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
    lager:info("Tx1 aborted. Test passed!"),
    pass.

%% @doc The following function tests the certification check algorithm,
%%  A transaction with snapshot time smaller than the prepare time of
%%  another transaction will abrot immediately.
clocksi_pending_prepare_abort_check(Nodes) ->
    lager:info("clockSI_pending_prepare_check started"),
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    %% Start a new tx,  perform an update over key write.
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, write, Type, {increment, 1}]),
    lager:info("Tx1 Writing..."),
    ?assertEqual(ok, WriteResult),

    %% Start a new tx,  perform an update over key write.
    {ok,TxId1}=rpc:call(LastNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1=rpc:call(LastNode, antidote, clocksi_iupdate,
                          [TxId1, write, Type, {increment, 2}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),
    lager:info("Tx1 finished concurrent write..."),

    %% prepare the first transaction.
    CommitTime=rpc:call(LastNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({ok, _}, CommitTime),

    %% Prepare the second transaction, which should be aborted.
    Result=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({aborted, TxId1}, Result),

    End1=rpc:call(LastNode, antidote, clocksi_icommit, [TxId]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx1 Committed."),
    lager:info("Tx1 aborted. Test passed!"),
    pass.


spawn_prepare(Node, TxId, Return) ->
    %% Prepare the second transaction, which should be aborted.
    Result=rpc:call(Node, antidote, clocksi_iprepare, [TxId]),
    lager:info("Prepare got result ~w",[Result]),
    Return ! {self(), Result}.


%% @doc The following function tests the certification check algorithm.
%%      when two concurrent txs modify a single object, one hast to abort.
%%      Besides, it updates multiple partitions.
clocksi_multiple_test_certification_check(Nodes) ->
    lager:info("clockSI_test_certification_check started"),
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("Node1: ~p", [FirstNode]),
    lager:info("LastNode: ~p", [LastNode]),
    Type = riak_dt_pncounter,
    %% Start a new tx,  perform an update over key write.
    {ok,TxId}=rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx1 Started, id : ~p", [TxId]),
    WriteResult=rpc:call(FirstNode, antidote, clocksi_iupdate,
                         [TxId, write, Type, {increment, 1}]),
    lager:info("Tx1 Writing 1..."),
    ?assertEqual(ok, WriteResult),
    WriteResultb=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, write2, Type, {increment, 1}]),
    lager:info("Tx1 Writing 2..."),
    ?assertEqual(ok, WriteResultb),
    WriteResultc=rpc:call(FirstNode, antidote, clocksi_iupdate,
                          [TxId, write3, Type, {increment, 1}]),
    lager:info("Tx1 Writing 3..."),
    ?assertEqual(ok, WriteResultc),

    %% Start a new tx,  perform an update over key write.
    {ok,TxId1}=rpc:call(LastNode, antidote, clocksi_istart_tx, []),
    lager:info("Tx2 Started, id : ~p", [TxId1]),
    WriteResult1=rpc:call(LastNode, antidote, clocksi_iupdate,
                          [TxId1, write, Type, {increment, 2}]),
    lager:info("Tx2 Writing..."),
    ?assertEqual(ok, WriteResult1),
    lager:info("Tx1 finished concurrent write..."),

    %% prepare and commit the second transaction.
    CommitTime1=rpc:call(LastNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertMatch({ok, _}, CommitTime1),
    lager:info("Tx2 sent prepare, got commitTime=..., id : ~p", [CommitTime1]),
    End1=rpc:call(LastNode, antidote, clocksi_icommit, [TxId1]),
    ?assertMatch({ok, _}, End1),
    lager:info("Tx2 Committed."),

    %% commit the first tx.
    CommitTime=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId]),
    ?assertMatch({aborted, TxId}, CommitTime),
    lager:info("Tx1 sent prepare, got message: ~p", [CommitTime]),
    lager:info("Tx1 aborted. Test passed!"),
    pass.
