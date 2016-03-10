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
-module(time_test).

-export([confirm/0, spawn_receive/5, nothing_after_wait/1, time_test1/2, time_test2/2, time_test3/2, time_test4/2]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([4]),
    PartList = get_part_list(8, hd(Nodes)), 
    lager:info("Nodes: ~p, PartList is ~p", [Nodes, PartList]),
    time_test1(Nodes, PartList),
    time_test2(Nodes, PartList),
    time_test3(Nodes, PartList),
    time_test4(Nodes, PartList),
    time_test5(Nodes, PartList),
    rt:clean_cluster(Nodes),
    pass.

%% Blind update of three txns to see if the timestamp of the second is affected by the first. 
time_test1(Nodes, PartList) ->
    lager:info("Test1 started"),
    Node1 = hd(Nodes),

    %% Update some keys and try to read
    Part1 = hd(PartList),
    TxId1 = {tx_id, 10000, self()}, 
    TxId2 = {tx_id, 100, self()},
    TxId3 = {tx_id, 0, self()},
    WS1 = [{Part1, [{t1_key1,1}]}],
    WS2 = [{Part1, [{t1_key2,1}]}],
    WS3 = [{Part1, [{t1_key3,1}]}],
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId1, local, self()]), 
    lager:info("Waiting for first prep"),
    receive
        Msg1 ->
            ?assertMatch({prepared, TxId1, 10001}, parse_msg(Msg1))
    end,

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2, TxId2, local, self()]), 
    lager:info("Waiting for second prep"),
    receive
        Msg2 ->
            ?assertMatch({prepared, TxId2, 101}, parse_msg(Msg2))
    end,

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS3, TxId3, local, self()]), 
    lager:info("Waiting for second prep"),
    receive
        Msg3 ->
            ?assertMatch({prepared, TxId3, 1}, parse_msg(Msg3))
    end,
    lager:info("Trying to commit first txn"),
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId1, 10001]), 
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId2, 101]), 
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId3, 1]), 

    {ok, Result1} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_key1, {tx_id, 10000, self()}]),
    ?assertEqual(Result1, []),
    {ok, Result2} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_key1, {tx_id, 10001, self()}]),
    ?assertEqual(Result2, 1),
    {ok, Result3} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_key2, {tx_id, 100, self()}]),
    ?assertEqual(Result3, []),
    {ok, Result4} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_key2, {tx_id, 101, self()}]),
    ?assertEqual(Result4, 1),
    {ok, Result5} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_key3, {tx_id, 0, self()}]),
    ?assertEqual(Result5, []),
    {ok, Result6} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, t1_key3, {tx_id, 1, self()}]),
    ?assertEqual(Result6, 1),
    pass.

%% Someone reads a key that does not intersect with my update set so doesn't affect my commit time 
time_test2(Nodes, PartList) ->
    lager:info("Test2 started"),
    Node1 = hd(Nodes),
    Node4 = lists:nth(4, Nodes),
    Node4ReplNode1 = list_to_atom(atom_to_list(Node4)++"repl"++atom_to_list(Node1)),

    K1 = t2_k1, K2 = t2_k2, K3 = t2_k3, K4 = t2_k4,
    Part1 = hd(PartList),
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    TxId2 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    TxId3 = {tx_id, 0, self()},
    WS1 = [{Part1, [{K3,1}, {K4, 1}]}],
    WS2 = [{Part1, [{K1,1}]}],
    WS3 = [{Part1, [{K2,1}]}],

    {ok, Result1} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K1, TxId1]),
    ?assertEqual(Result1, []),
    {ok, Result2} = rpc:call(Node4, data_repl_serv, read, [Node4ReplNode1, K2, TxId2, Part1]),
    ?assertEqual(Result2, []),

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId3, local, self()]),
    receive
        Msg1 ->
            ?assertMatch({prepared, TxId3, 1}, parse_msg(Msg1))
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId3, 1]),

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2, TxId1, local, self()]),
    {tx_id, SnapshotTime1, _} = TxId1,
    PrepTime1 = SnapshotTime1+1,
    receive
        Msg2 ->
            ?assertMatch({prepared, TxId1, PrepTime1}, parse_msg(Msg2))
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId1, 1]),

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS3, TxId2, local, self()]),
    {tx_id, SnapshotTime2, _} = TxId2,
    PrepTime2 = SnapshotTime2+1,
    receive
        Msg3 ->
            ?assertMatch({prepared, TxId2, PrepTime2}, parse_msg(Msg3))
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId2, 1]),
    pass.

time_test3(Nodes, PartList) ->
    lager:info("Test3 started"),
    Node1 = hd(Nodes),

    K1 = t3_k1, K2 = t3_k2,
    Part1 = hd(PartList),
    Part2 = lists:nth(2, PartList),
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime1, _} = TxId1,
    PrepTime1 = SnapshotTime1 + 1,
    TxId2 = {tx_id, 0, self()},
    TxId3 = {tx_id, 1, self()},
    TxId4 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime4, _} = TxId4,
    PrepTime4 = SnapshotTime4 + 1,
    WS1 = [{Part1, [{K1,1}]}, {Part2, [{K2,1}]}],
    WS2 = [{Part1, [{K1,1}]}],

    {ok, Result1} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K1, TxId1]),
    ?assertEqual(Result1, []),

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId2, local, self()]),
    receive
        Msg1 -> case parse_msg(Msg1) of
                    {prepared, TxId2, 1} ->
                        receive
                            Msg2 ->  ?assertEqual({prepared, TxId2, SnapshotTime1+1}, parse_msg(Msg2))
                        end;
                    {prepared, TxId2, PrepTime1} ->
                          receive
                              Msg2 ->  ?assertEqual({prepared, TxId2, 1}, parse_msg(Msg2))
                          end
                end
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1, Part2], TxId2, PrepTime1]),
    
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2, TxId3, local, self()]),
    receive
          Msg3 -> 
                ?assertMatch({aborted, TxId3, _}, parse_msg(Msg3))
    end,

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2, TxId4, local, self()]),
    receive
          Msg4 -> 
                ?assertEqual({prepared, TxId4, PrepTime4}, parse_msg(Msg4))
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId4, PrepTime4]),

    pass.

%% Commit a key and then read.. To see if the key will be updated with the correct timestamp.
%% Read from both the vnode and the replica.
time_test4(Nodes, PartList) ->
    lager:info("Test4 started"),
    Node1 = hd(Nodes),
    Node2 = lists:nth(2, Nodes),
    Node3 = lists:nth(3, Nodes),
    Node4 = lists:nth(4, Nodes),

    K1 = t4_k1, K2 = t4_k2,
    Part1 = hd(PartList),
    Part1Rep1 = list_to_atom(atom_to_list(Node4)++"repl"++atom_to_list(Node1)), 
    Part1Rep2 = list_to_atom(atom_to_list(Node3)++"repl"++atom_to_list(Node1)), 
    Part2 = lists:nth(2, PartList),
    Part2Rep1 = list_to_atom(atom_to_list(Node2)++"repl"++atom_to_list(Node4)), 
    Part2Rep2 = list_to_atom(atom_to_list(Node3)++"repl"++atom_to_list(Node4)), 
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime1, _} = TxId1,
    PrepTime1 = SnapshotTime1 + 1,

    TxId2 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime2, _} = TxId2,
    PrepTime2 = SnapshotTime2 + 1,

    TxId3 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime3, _} = TxId3,
    PrepTime3 = SnapshotTime3 + 1,

    TxId4 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime4, _} = TxId4,
    PrepTime4 = SnapshotTime4 + 1,

    {ok, []} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K1, TxId2]),
    {ok, []} = rpc:call(Node4, data_repl_serv, read, [Part1Rep1, K1, TxId3, Part1]),
    {ok, []} = rpc:call(Node3, data_repl_serv, read, [Part1Rep2, K1, TxId4, Part1]),

    lager:info("Time 1 ~w, 2: ~w, 3: ~w, 4:~w", [PrepTime1, PrepTime2, PrepTime3, PrepTime4]),
    MaxPrepTime = max(max(max(PrepTime1, PrepTime2), PrepTime3), PrepTime4),
    WS1 = [{Part1, [{K1,1}]}],
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId1, local_only, self()]),
    receive
        Msg1 -> 
            ?assertEqual({prepared, TxId1, MaxPrepTime}, parse_msg(Msg1))
    end,

    TxId5 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime5, _} = TxId5,
    PrepTime5 = SnapshotTime5 + 1,

    TxId6 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime6, _} = TxId6,
    PrepTime6 = SnapshotTime6 + 1,

    TxId7 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime7, _} = TxId7,
    PrepTime7 = SnapshotTime7 + 1,

    TxId8 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime8, _} = TxId8,
    PrepTime8 = SnapshotTime8 + 1,

    lager:info("Time 5 ~w, 6: ~w, 7: ~w, 8:~w", [PrepTime5, PrepTime6, PrepTime7, PrepTime8]),
    {ok, []} = rpc:call(Node4, clocksi_vnode, read_data_item, [Part2, K2, TxId6]),
    {ok, []} = rpc:call(Node2, data_repl_serv, read, [Part2Rep1, K2, TxId7, Part1]),
    {ok, []} = rpc:call(Node3, data_repl_serv, read, [Part2Rep2, K2, TxId8, Part1]),

    MaxPrepTime1 = max(max(max(PrepTime5, PrepTime6), PrepTime7), PrepTime8),
    WS2 = [{Part2, [{K2,1}]}],
    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS2, TxId5, local_only, self()]),
    receive
        Msg2 -> 
            ?assertEqual({prepared, TxId5, MaxPrepTime1}, parse_msg(Msg2))
    end,
    pass.
    
%% Commits with a high timestamp and then try to commit the txn again..
%% One should get aborted and one gets committed.
time_test5(Nodes, PartList) ->
    lager:info("Test5 started"),
    Node1 = hd(Nodes),

    K1 = t5_k1,
    Part1 = hd(PartList),
    TxId0 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    TxId1 = rpc:call(Node1, tx_utilities, create_tx_id, [0]),
    {tx_id, SnapshotTime1, _} = TxId1,
    PrepTime1 = SnapshotTime1 + 1,
    TxId2 = {tx_id, PrepTime1, self()},
    PrepTime2 = SnapshotTime1 + 2,
    WS1 = [{Part1, [{K1,1}]}],

    %{ok, Result1} = rpc:call(Node1, clocksi_vnode, read_data_item, [Part1, K1, TxId2]),
    %?assertEqual(Result1, []),

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId1, local_only, self()]),
    receive
        Msg1 -> 
            ?assertEqual({prepared, TxId1, PrepTime1}, parse_msg(Msg1))
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId1, PrepTime1]),

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId0, local_only, self()]),
    receive
        Msg2 -> 
            ?assertMatch({aborted, TxId0, _}, parse_msg(Msg2))
    end,

    rpc:call(Node1, clocksi_vnode, debug_prepare, [WS1, TxId2, local_only, self()]),
    receive
        Msg3 -> 
            ?assertEqual({prepared, TxId2, PrepTime2}, parse_msg(Msg3))
    end,
    rpc:call(Node1, clocksi_vnode, commit, [[Part1], TxId2, PrepTime2]),
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
