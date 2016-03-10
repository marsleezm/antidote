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
-module(basic_test).

-export([confirm/0, spawn_receive/5]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([4]),
    lager:info("Nodes: ~p", [Nodes]),
    basic_test1(Nodes),
    basic_test2(Nodes),
    basic_test3(Nodes),
    rt:clean_cluster(Nodes),
    pass.

%% No concurrency
basic_test1(Nodes) ->
    lager:info("Test1 started"),
    Node1 = hd(Nodes),
    %% Update only local partition
    TxId1 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    [Part1] = rpc:call(Node1, hash_fun, get_preflist_from_key, [1]), 
    LUps = [{Part1, [{t1_k1, 1}, {t1_k2, 2}, {t1_k3, 3}]}],
    {ok, Result1} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId1, LUps, []]),
    ?assertMatch({committed, _}, Result1),
    timer:sleep(500),
    TxId2 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    ReadResult1 = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_k1, TxId2]), 
    ?assertEqual({ok, 1}, ReadResult1),
    
    %% Update only remote partition.
    TxId3 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    RUps = [{Part1, [{t1_k1, 2}, {t1_k2, 4}, {t1_k3, 6}]}],
    {ok, Result2} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId3, [], RUps]),
    ?assertMatch({_, _}, Result2),
    timer:sleep(100),

    TxId4 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    ReadResult2 = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_k1, TxId4]), 
    ?assertEqual({ok, 2}, ReadResult2).

%% Test snapshot
basic_test2(Nodes) ->
    lager:info("Test2 started"),
    Node1 = hd(Nodes),
    TxId1 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    [Part1] = rpc:call(Node1, hash_fun, get_preflist_from_key, [1]),
    LUps = [{Part1, [{t2_k1, 1}, {t2_k2, 2}, {t2_k3, 3}]}],
    {ok, Result1} = rpc:call(Node1, tx_cert_sup, certify, [1, TxId1, LUps, []]),
    ?assertMatch({committed, _}, Result1),

    ReadResult1 = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t2_k1, TxId1]),
    ?assertEqual({ok, []}, ReadResult1),

    TxId2 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    ReadResult2 = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t2_k1, TxId2]),
    ?assertEqual({ok, 1}, ReadResult2).
      

%% Blocked read 
basic_test3(Nodes) ->
    lager:info("Test3 started"),
    Node1 = hd(Nodes),
    TxId1 = rpc:call(Node1, tx_cert_sup, start_tx, [1]),
    [Part1] = rpc:call(Node1, hash_fun, get_preflist_from_key, [1]),
    LUps = [{Part1, [{t3_k1, 1}, {t3_k2, 2}, {t3_k3, 3}]}],
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [Part1, true]),
    lager:info("TxId1 is ~w", [TxId1]),
    spawn(specula_test, spawn_receive, [self(), Node1, tx_cert_sup, certify, [1, TxId1, LUps, []]]),

    timer:sleep(1000),
    ReadResult1 = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t3_k1, TxId1]),
    ?assertEqual({ok, []}, ReadResult1),
    lager:info("After first read"),

    TxId2 = rpc:call(Node1, tx_utilities, create_tx_id, [1]),
    lager:info("TxId2 is ~w", [TxId2]),
    lager:info("Before spawnning!"),
    spawn(specula_test, spawn_receive, [self(), Node1, clocksi_vnode, read_data_item, [Part1, t3_k1, TxId2]]),
    lager:info("Before do reply! TxId is ~w", [TxId1]),
    ok = rpc:call(Node1, clocksi_vnode, do_reply, [Part1, TxId1]),
    lager:info("Got reply!"),

    receive 
        {ok, {committed, _}} ->
            %lager:info("Got committed"),
            ok;
        {ok, V} ->
            ?assertEqual(1, V)
    end,
    ok = rpc:call(Node1, clocksi_vnode, set_debug, [Part1, false]).

spawn_receive(Myself, Node, Mod, Fun, Param) ->
    Result = rpc:call(Node, Mod, Fun, Param),
    Myself ! Result.
