%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
-module(micro_load).

-export([load/1, thread_load/4]).

-include("tpcc.hrl").

-define(TIMEOUT, 20000).

%% ====================================================================
%% API
%% ====================================================================

load(Size) ->
    lager:info("Start tpcc load at ~w", [node()]),
    random:seed(now()), %now()),

    {PartList, ReplList, _NumDcs} =  hash_fun:get_hash_fun(), %gen_server:call({global, MyTxServer}, {get_hash_fun}),
    %lager:info("Part list is ~w, Replist is ~w", [PartList, ReplList]),
    AllDcs = [N || {N, _} <- PartList],
    MyNode = node(),
    [M] = [L || {N, L} <- ReplList, N == MyNode ],
    MyRepIds = get_indexes(M, AllDcs),
    _FullPartList = lists:flatten([L || {_, L} <- PartList]),
    MyReps = lists:map(fun(Index) ->  Name = lists:nth(Index, AllDcs),  {Index, {rep, get_rep_name(MyNode, Name)}} end, MyRepIds),
    DcId = index(MyNode, AllDcs),
    lager:info("TargetNode is ~p, DcId is ~w, My Replica Ids are ~w",[MyNode, DcId, MyReps]),
    StartTime = os:timestamp(),
    
    ToPopulateParts = [{DcId, server}|MyReps],
    lists:foreach(fun({Id, Server}) -> 
        lager:info("Spawning for dc ~w", [Id]),
        Tables = case Server of server -> 
                                    {_, L} = lists:nth(DcId, PartList),
                                    lists:foldl(fun(P, Acc) ->  Tab = clocksi_vnode:get_table(P), Acc++[Tab] end, [], L);
                                {rep, S} -> data_repl_serv:get_table(S)
                end,
        spawn(micro_load, thread_load, [Id, Tables, Size, self()]) end, ToPopulateParts),
    lager:info("~p waiting for children..", [self()]),
    lists:foreach(fun(_) ->  receive done -> ok, lager:info("Receive a reply") end end, ToPopulateParts), 
    EndTime = os:timestamp(),
    lager:info("Population finished.. used ~w", [timer:now_diff(EndTime, StartTime)/1000000]).

thread_load(_DcId, Server, Size, Sup) ->
    lager:info("Started in thread load! My tables are ~w", [Server]),
    put_range_items(Server, Size),
    Sup ! done.
    
put_range_items(TxServer, Size) ->
    lager:info("Populating items from ~w to ~w", [1, Size]),
    Seq = lists:seq(1, Size),
    lists:foreach(fun(ItemId) ->
                    put_to_node(TxServer, ItemId, ItemId*100)
                    end, Seq).
    
put_to_node(Tabs, Key, Value) ->
    CommitTime = 1,
    case is_list(Tabs) of
        true -> %% Is clocksi table
            Index = Key rem length(Tabs) + 1,
            Tab = lists:nth(Index, Tabs),
            %lager:info("Putting ~p ~p to ~w", [Key, Value, Tab]),
            ets:insert(Tab, {Key, [{CommitTime, Value}]});
        false ->
            ets:insert(Tabs, {Key, [{CommitTime, Value}]})
    end.

index(Elem, L) ->
    index(Elem, L, 1).

index(_, [], _) ->
    -1;
index(E, [E|_], N) ->
    N;
index(E, [_|L], N) ->
    index(E, L, N+1).

get_indexes(PL, List) ->
    %lager:info("Trying to get index: PL ~w, List ~w", [PL, List]),
    [index(X, List) || X <- PL ].

get_rep_name(Target, Rep) ->
    list_to_atom(atom_to_list(Target)++"repl"++atom_to_list(Rep)).

