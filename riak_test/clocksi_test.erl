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
-module(clocksi_test).

-export([confirm/0, spawn_receive/5]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([4]),
    lager:info("Nodes: ~p", [Nodes]),
    PartList = get_part_list(5, hd(Nodes)), 
    clocksi_test1(Nodes, PartList),
    clocksi_test2(Nodes, PartList),
    clocksi_test3(Nodes, PartList),
    clocksi_test4(Nodes, PartList),
    rt:clean_cluster(Nodes),
    pass.

%% No concurrency
clocksi_test1(Nodes, PartList) ->
    lager:info("Test1 started"),
    Node1 = hd(Nodes),

    %% Update some keys and try to read
    Part1 = hd(PartList),
    Part2 = lists:nth(2, PartList),
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    WS1 = [{Part1, [{t1_key1,1}, {t1_key2, 2}]}, {Part2, [{t1_key3,3},{t1_key4,4}]}],
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId1, local, self()]), 
    lager:info("Waiting for first prep"),
    receive
        Msg1 ->
            ?assertMatch({prepared, TxId1, _}, parse_msg(Msg1))
    end,
    lager:info("Waiting for second prep"),
    receive
        Msg2 ->
            ?assertMatch({prepared, TxId1, _}, parse_msg(Msg2))
    end,
    {tx_id, SnapshotTime, _} = TxId1,
    lager:info("Trying to commit first txn"),
    rpc:call(Node1, clocksi_vnode, commit, [[Part1, Part2], TxId1, SnapshotTime+10]), 
    {ok, Result1} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_key1, {tx_id, SnapshotTime+10, self()}]),
    ?assertEqual(Result1, 1),
    {ok, Result2} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part2, t1_key3, {tx_id, SnapshotTime+11, self()}]),
    ?assertEqual(Result2, 3),
    {ok, Result3} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_key2, {tx_id, SnapshotTime, self()}]),
    ?assertEqual(Result3, []),
    {ok, Result4} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part2, t1_key4, {tx_id, SnapshotTime+9, self()}]),
    ?assertEqual(Result4, []),
    pass.

%% Multiple read dependency
clocksi_test2(Nodes, PartList) ->
    lager:info("Test2 started"),
    Node1 = hd(Nodes),
    
    K1 = t2_k1, K2 = t2_k2, K3 = t2_k3, K4 = t2_k4,
    Part1 = hd(PartList),
    Part2 = lists:nth(2, PartList),
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    WS1 = [{Part1, [{K1,1}, {K2, 2}]}, {Part2, [{K3,3},{K4,4}]}],
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId1, local, self()]),
    receive
        Msg1 ->
            ?assertMatch({prepared, TxId1, _}, parse_msg(Msg1))
    end,
    receive
        Msg2 ->
            ?assertMatch({prepared, TxId1, _}, parse_msg(Msg2))
    end,
    {tx_id, SnapshotTime, _} = TxId1,
    lager:info("Before reading ~w, TxId is ~w", [K1, TxId1]),
    {ok, Result} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K1, {tx_id, SnapshotTime, self()}]),
    ?assertEqual(Result, []),
    lager:info("Got result1 is ~w", [Result]),
    spawn(clocksi_test, spawn_receive, [self(), Node1, clocksi_vnode, read_data_item, [Part1, K1, {tx_id, SnapshotTime+9, self()}]]),
    spawn(clocksi_test, spawn_receive, [self(), Node1, clocksi_vnode, read_data_item, [Part2, K3, {tx_id, SnapshotTime+10, self()}]]),
    receive
        Result0 ->
            lager:info("Received ~w", [Result0])
    after
        1000 ->
            lager:info("Didn't receive anything before commit")
    end,
    lager:info("Before commit"),
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId1, SnapshotTime+10]),
    receive
        Result1 ->
            lager:info("Receive ~w after commit", [Result1]),
            ?assertEqual(Result1, {ok, []})
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part2], TxId1, SnapshotTime+10]),
    receive
        Result2 ->
            lager:info("Receive ~w after commit", [Result2]),
            ?assertEqual(Result2, {ok, 3})
    end,
    pass.
    
%% Multiple update dependency
clocksi_test3(Nodes, PartList) ->
    lager:info("Test3 started"),
    Node1 = hd(Nodes),

    K1 = t3_k1, K2 = t3_k2, K3 = t3_k3, K4 = t3_k4,
    Part1 = hd(PartList),
    Part2 = lists:nth(2, PartList),
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime, _} = TxId1,
    lager:info("TxId 1 is ~w", [TxId1]),
    TxId2 = {tx_id, SnapshotTime+10, self()}, 
    lager:info("TxId 2 is ~w", [TxId2]),
    WS1P1 = [{Part1, [{K1,1}, {K2,2}]}], 
    WS1P2 = [{Part2, [{K3,3}, {K4,4}]}],
    WS2P1 = [{Part1, [{K1,10}, {K2, 20}]}], 
    WS2P2 = [{Part2, [{K3,30}, {K4,40}]}],
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1P1, TxId1, local, self()]),
    receive
        Msg1 ->
            ?assertMatch({prepared, TxId1, _}, parse_msg(Msg1))
    end,
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1P2, TxId1, local, self()]),
    receive
        Msg2 ->
            ?assertMatch({prepared, TxId1, _}, parse_msg(Msg2))
    end,
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2P1, TxId2, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2P2, TxId2, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, commit, [[Part1, Part2], TxId1, SnapshotTime+5]),
    lager:info("Should receive prepared!!!"),
    receive
        Msg3 ->
            ?assertMatch({prepared, TxId2, _}, parse_msg(Msg3))
    end,
    receive
        Msg4 ->
            ?assertMatch({prepared, TxId2, _}, parse_msg(Msg4))
    end,

    %% Deps got aborted 
    TxId3 = {tx_id, SnapshotTime+15, self()}, 
    lager:info("TxId 3 is ~w, ws2p2 is ~w", [TxId3, WS2P2]),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2P2, TxId3, local, self()]),
    rpc:call(Node1, clocksi_vnode, commit, [[Part1, Part2], TxId2, SnapshotTime+16]),
    receive
        Msg5 ->
            lager:warning("Received aborted for ~w, Msg is ~w", [TxId3, Msg5]),
            ?assertMatch({aborted, TxId3, _}, parse_msg(Msg5))
    end,

    %% Concurrency test.. Two concurrent transaction sending reads, one wait and the other abort, so no deadlock can happen
    TxId4 = {tx_id, SnapshotTime+20, self()}, 
    TxId5 = {tx_id, SnapshotTime+25, self()}, 
    WS4P1 = [{Part1, [{K1,41}, {K2,41}]}], 
    WS4P2 = [{Part2, [{K3,43}, {K4,44}]}],
    WS5P1 = [{Part1, [{K1,51}, {K2,52}]}], 
    WS5P2 = [{Part2, [{K3,53}, {K4,54}]}],
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS4P1, TxId4, local, self()]),
    receive
        Msg6 ->
            ?assertMatch({prepared, TxId4, _}, parse_msg(Msg6))
    end,
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS5P1, TxId5, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS5P2, TxId5, local, self()]),
    receive
        Msg7 ->
            ?assertMatch({prepared, TxId5, _}, parse_msg(Msg7))
    end,
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS4P2, TxId4, local, self()]),
    receive
       Msg8 ->
            ?assertMatch({aborted, TxId4, _}, parse_msg(Msg8))
    end,
    lager:info("Tx4 with ~w should aborted already", [TxId4]),
    %% A non-trivial case for waiting: 
    %% In part1, the queue is: TxId5, [TxId6, TxId7, TxId8]
    %% In part2, the queue is: TxId5, [TxId7, TxId6, TxId8]
    %% The commit of TxId5 will abort TxId6 but prepare TxId7.. The commit of TxId7 will prepare TxId8
    lager:info("Non trivial test"),
    TxId6 = {tx_id, SnapshotTime+30, self()}, 
    TxId7 = {tx_id, SnapshotTime+35, self()}, 
    TxId8 = {tx_id, SnapshotTime+40, self()}, 
    lager:info("TxId6 is ~w", [TxId6]),
    lager:info("TxId7 is ~w", [TxId7]),
    lager:info("TxId8 is ~w", [TxId8]),
    WS6P1 = [{Part1, [{K1,61}, {K2,61}]}], 
    WS6P2 = [{Part2, [{K3,63}, {K4,64}]}],
    WS7P1 = [{Part1, [{K1,71}, {K2,72}]}], 
    WS7P2 = [{Part2, [{K3,73}, {K4,74}]}],
    WS8P1 = [{Part1, [{K1,81}, {K2,82}]}], 
    WS8P2 = [{Part2, [{K3,83}, {K4,84}]}],
    rpc:call(Node1, clocksi_vnode, abort, [[Part1, Part2], TxId4]),
    receive
       Msgx ->
            ?assertMatch({prepared, TxId5, _}, parse_msg(Msgx))
    end,
    lager:info("Aborted Tx 4 test"),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS6P1, TxId6, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS7P1, TxId7, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS8P1, TxId8, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS7P2, TxId7, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS6P2, TxId6, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS8P2, TxId8, local, self()]),
    true = nothing_after_wait(500),
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId5, SnapshotTime+33]),
    receive Msg9 ->
        case parse_msg(Msg9) of
            {aborted, _, _} ->
                ?assertMatch({aborted, TxId6, _}, parse_msg(Msg9)),
                receive Msg10 -> ?assertMatch({prepared, TxId7, _}, parse_msg(Msg10)) end;
            _ ->
                ?assertMatch({prepared, TxId7, _}, parse_msg(Msg9)),
                receive Msg10 -> ?assertMatch({aborted, TxId6, _}, parse_msg(Msg10)) end
        end
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part2], TxId5, SnapshotTime+33]),
    receive Msg11 ->
        case parse_msg(Msg11) of
            {aborted, _, _} ->
                ?assertMatch({aborted, TxId6, _}, parse_msg(Msg11)),
                receive Msg12 -> ?assertMatch({prepared, TxId7, _}, parse_msg(Msg12)) end;
            _ ->
                ?assertMatch({prepared, TxId7, _}, parse_msg(Msg11)),
                receive Msg12 -> ?assertMatch({aborted, TxId6, _}, parse_msg(Msg12)) end
        end
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1, Part2], TxId7, SnapshotTime+37]),
    receive
        Msg13 ->
            ?assertMatch({prepared, TxId8, _}, parse_msg(Msg13))
    end,
    receive
        Msg14 ->
            ?assertMatch({prepared, TxId8, _}, parse_msg(Msg14))
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1, Part2], TxId8, SnapshotTime+45]),
    {ok, Result8} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K1, {tx_id, SnapshotTime+45, self()}]),
    ?assertEqual(Result8, 81),
    {ok, Result9} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part2, K3, {tx_id, SnapshotTime+45, self()}]),
    ?assertEqual(Result9, 83),
    pass.

%% A transaction has multi-dependency 
clocksi_test4(Nodes, PartList) ->
    lager:info("Test4 started"),
    Node1 = hd(Nodes),

    K1 = t4_k1, K2 =t4_k2, K3 = t4_k3, K4 = t4_k4,
    Part1 = hd(PartList),
    Part2 = lists:nth(2, PartList),
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime, _} = TxId1,
    lager:info("TxId 1 is ~w", [TxId1]),
    TxId2 = {tx_id, SnapshotTime-5, self()},
    TxId3 = {tx_id, SnapshotTime+10, self()},
    TxId4 = {tx_id, SnapshotTime+20, self()},
    lager:info("TxId 2 is ~w", [TxId2]),
    lager:info("TxId 3 is ~w", [TxId3]),
    WS1 = [{Part1, [{K1,1}, {K2,2}, {K3,3}, {K4,4}]}],
    WS2 = [{Part1, [{K1,11}]}],
    WS3 = [{Part1, [{K4,13}]}],
    WS4 = [{Part1, [{K1,41}, {K2,42}, {K3,43}, {K4,44}]}],
    lager:info("TxId 2 is ~w", [TxId2]),
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2, TxId2, local, self()]),
    receive
        Msg1 ->
            ?assertMatch({prepared, TxId2, _}, parse_msg(Msg1))
    end,
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS3, TxId3, local, self()]),
    receive
        Msg2 ->
            ?assertMatch({prepared, TxId3, _}, parse_msg(Msg2))
    end,
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId1, local, self()]),
    receive
        Msg3 ->
            ?assertMatch({aborted, TxId1, _}, parse_msg(Msg3))
    end,
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS4, TxId4, local, self()]),
    nothing_after_wait(500), 
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId2, SnapshotTime+15]),
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId3, SnapshotTime+20]),
    receive
        Msg4 ->
            ?assertMatch({prepared, TxId4, _}, parse_msg(Msg4))
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1, Part2], TxId4, SnapshotTime+25]),
    {ok, Result1} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K1, {tx_id, SnapshotTime+24, self()}]),
    ?assertEqual(Result1, 11),
    {ok, Result2} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K1, {tx_id, SnapshotTime+25, self()}]),
    ?assertEqual(Result2, 41),
    {ok, Result3} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K3, {tx_id, SnapshotTime+24, self()}]),
    ?assertEqual(Result3, []),
    {ok, Result4} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K3, {tx_id, SnapshotTime+25, self()}]),
    ?assertEqual(Result4, 43),
    lager:info("Should receive prepared!!!"),
    pass.


%%%%%%%% Private fun %%%%%%%%%%
get_part_list(Num, Node) ->
    lists:foldl(fun(Key, PartList) ->
                    [Part] = rpc:call(Node, hash_fun, get_preflist_from_key, [Key]),
                    [Part|PartList]
                    end, [], lists:seq(1, Num)).

spawn_receive(Myself, Node, Mod, Fun, Param) ->
    Result = rpc:call(Node, Mod, Fun, Param),
    Myself ! Result.

parse_msg({_, RealMsg}) ->
    RealMsg.

nothing_after_wait(MSec) ->
    receive
        UnExpMsg ->
            lager:info("Received unexpected message ~w", [UnExpMsg]),
            false
    after
        MSec ->
            lager:info("Didn't receive anything after ~w msec", [MSec]),
            true
    end.
