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
-module(hash_fun).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-export([get_preflist_from_key/1,
         build_rep_dict/1,
         get_my_next/2,
         get_partitions/0,
         build_rev_replicas/0,
         get_local_vnode_by_id/1,
         get_vnode_by_id/2,
         get_my_previous/2,
         get_logid_from_key/1,
         get_local_servers/0,
         remove_node_from_preflist/1,
         get_hash_fun/0,
         init_hash_fun/0,
         get_my_node/1
        ]).

%% @doc get_logid_from_key computes the log identifier from a key
%%      Input:  Key:    The key from which the log id is going to be computed
%%      Return: Log id
%%
-spec get_logid_from_key(key()) -> log_id().
get_logid_from_key(Key) ->
    %HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    PreflistAnn = get_preflist_from_key(Key),
    remove_node_from_preflist(PreflistAnn).

%% @doc get_preflist_from_key returns a preference list where a given
%%      key's logfile will be located.
-spec get_preflist_from_key(key()) -> preflist().
get_preflist_from_key(Key) ->
    ConvertedKey = convert_key(Key),
    %HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
    get_primaries_preflist(ConvertedKey).

%% @doc get_primaries_preflist returns the preflist with the primary
%%      vnodes. No matter they are up or down.
%%      Input:  A hashed key
%%      Return: The primaries preflist
%%
-spec get_primaries_preflist(non_neg_integer()) -> preflist().
get_primaries_preflist(Key)->
    PartitionList = get_partitions(),
    Pos = Key rem length(PartitionList) + 1,
    [lists:nth(Pos, PartitionList)].

get_local_vnode_by_id(Index) ->
    lists:nth(Index, get_local_servers()).

get_vnode_by_id(Index, NodeIndex) ->
    lists:nth(Index, get_node_parts(NodeIndex)).

-spec get_node_parts(integer()) -> [term()].
get_node_parts(NodeIndex) ->
    case ets:lookup(meta_info, NodeIndex) of
        [{NodeIndex, PartList}] ->
            %lager:info("NodeIndex is ~w, PartList is ~w", [NodeIndex, PartList]),
            PartList;
        [] ->
            lager:warning("Geting part node.. Something is wrong!!!")
    end.

-spec get_local_servers() -> [term()].
get_local_servers() ->
    case ets:lookup(meta_info, local_node) of
        [{_, PartList}] ->
            PartList;
        [] ->
            lager:info("Something is wrong!!")
    end.

-spec get_my_previous(chash:index_as_int(), non_neg_integer()) -> preflist().
get_my_previous(Partition, N) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    Size = chashbin:num_partitions(CHBin),
    Itr = chashbin:iterator(Partition, CHBin),
    {Primaries, _} = chashbin:itr_pop(Size-1, Itr),
    lists:sublist(Primaries, Size-N, N).

-spec get_my_next(chash:index_as_int(), non_neg_integer()) -> preflist().
get_my_next(Partition, N) ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    Itr = chashbin:iterator(Partition, CHBin),
    {Primaries, _} = chashbin:itr_pop(N, Itr),
    Primaries.

get_my_node(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:index_owner(Ring, Partition).

%% @doc remove_node_from_preflist: From each element of the input
%%      preflist, the node identifier is removed
%%      Input:  Preflist: list of pairs {Partition, Node}
%%      Return: List of Partition identifiers
%%
-spec remove_node_from_preflist(preflist()) -> [partition_id()].
remove_node_from_preflist(Preflist) ->
    F = fun({P,_}) -> P end,
    lists:map(F, Preflist).

%% @doc Convert key. If the key is integer(or integer in form of binary),
%% directly use it to get the partition. If it is not integer, convert it
%% to integer using hash.
-spec convert_key(key()) -> non_neg_integer().
convert_key(Key) ->
    case is_binary(Key) of
        true ->
            KeyInt = (catch list_to_integer(binary_to_list(Key))),
            case is_integer(KeyInt) of 
                true -> abs(KeyInt);
                false ->
                    HashedKey = riak_core_util:chash_key({?BUCKET, Key}),
                    abs(crypto:bytes_to_integer(HashedKey))
            end;
        false ->
            case is_integer(Key) of 
                true ->
                    abs(Key);
                false ->
                    HashedKey = riak_core_util:chash_key({?BUCKET, term_to_binary(Key)}),
                    abs(crypto:bytes_to_integer(HashedKey))
            end
    end.

get_partitions() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    chashbin:to_list(CHBin).

get_hash_fun() ->
    %case ets:lookup(meta_info, node_list) of
    %    [{node_list, List}] ->
    %        case antidote_config:get(do_repl) of
    %            true ->
    %                {List, antidote_config:get(to_repl)};
    %            false ->
    %                {List, []}
    %        end;
    %    [] ->
            %case ets:lookup(meta_info, node_list) of
            %    [{node_list, List}] ->
                init_hash_fun(),
                Partitions = get_partitions(),
                Dict1 = lists:foldl(fun({Index, Node}, Dict) ->
                    case dict:find(Node, Dict) of
                        error ->
                            dict:store(Node, [{Index,Node}], Dict);
                        {ok, List} ->
                            dict:store(Node, List++[{Index, Node}], Dict)
                    end
                end, dict:new(), Partitions),
                List = lists:reverse(dict:to_list(Dict1)),
                case antidote_config:get(do_repl) of
                    true ->
                        {List, antidote_config:get(to_repl), antidote_config:get(num_dcs)};
                    false ->
                        {List, [], antidote_config:get(num_dcs)}
                end.
            %end.
    %end.

init_hash_fun() ->
    Partitions = get_partitions(),
    Dict1 = lists:foldl(fun({Index, Node}, Dict) ->
                    case dict:find(Node, Dict) of   
                        error ->
                            dict:store(Node, [{Index,Node}], Dict);
                        {ok, List} ->
                            dict:store(Node, List++[{Index, Node}], Dict)
                    end 
                end, dict:new(), Partitions),
    PartitionListByNode = lists:reverse(dict:to_list(Dict1)),
    ets:insert(meta_info, {node_list, PartitionListByNode}),
    lists:foreach(fun({Node, PartList}) ->
        case node() of
            Node ->
                 ets:insert(meta_info, {local_node, PartList});
            _ ->
                 ok
        end end, PartitionListByNode),
    PartitionListByNode.

build_rev_replicas() ->
    Lists = antidote_config:get(to_repl),
    AllNodes = [Node  || {Node, _} <- Lists],
    lists:foreach(fun(N) ->
            ReplList = {N, [list_to_atom(atom_to_list(Node)++"repl"++atom_to_list(N)) 
                            ||  {Node, ToRepl} <- Lists, if_repl_myself(ToRepl, N)]},
            lager:info("Inserting ~w to repl table", [ReplList]),
            ets:insert(meta_info, ReplList)
        end, AllNodes).

build_rep_dict(false)->
    dict:new();
build_rep_dict(true) ->
    Lists = antidote_config:get(to_repl),
    AllNodes = [Node  || {Node, _} <- Lists],
    ReverseList = lists:foldl(fun(RepedNode, L) ->
            RepList = {RepedNode, [RepingNode
                            ||  {RepingNode, ToRepl} <- Lists, if_repl_myself(ToRepl, RepedNode)]},
            [RepList|L]
        end, [], AllNodes),
    MyNode = node(),
    [RepNodes] = [Reps || {Node, Reps} <- Lists, Node == node()],
    RepDict = lists:foldl(fun(N, D) -> dict:store({rep, N},
                    list_to_atom(atom_to_list(node())++"repl"++atom_to_list(N)), D) end, dict:new(), RepNodes),
    Lists = antidote_config:get(to_repl),
    RepDict1 = lists:foldl(fun(RepedNode, R) ->
                     {RepedNode, NodeRepList} = lists:keyfind(RepedNode, 1, ReverseList),
                     RepList = lists:foldl(fun(RepingNode, L) ->
                                        case RepingNode of
                                            MyNode ->
                                                [{rep, list_to_atom(atom_to_list(RepingNode)++"repl"++atom_to_list(RepedNode))}|L];
                                                %[list_to_atom(atom_to_list(RepingNode)++"repl"++atom_to_list(RepedNode))|L];
                                            _ ->
                                                [list_to_atom(atom_to_list(RepingNode)++"repl"++atom_to_list(RepedNode))|L]
                                        end end, [], NodeRepList),
                    dict:store(RepedNode, RepList, R)
                 end,  RepDict, AllNodes),
    AllButMe = AllNodes -- [MyNode],
    CacheNode = AllButMe -- RepNodes,
    lists:foldl(fun(N, D) ->
              dict:store(N, cache, D)
            end, RepDict1, CacheNode).

if_repl_myself([], _) ->
    false;
if_repl_myself([N|_], N) ->
    true;
if_repl_myself([_|Rest], N) ->
    if_repl_myself(Rest, N).

-ifdef(TEST).
%% @doc Testing remove_node_from_preflist
remove_node_from_preflist_test()->
    Preflist = [{partition1, node},
                {partition2, node},
                {partition3, node}],
    ?assertEqual([partition1, partition2, partition3],
                 remove_node_from_preflist(Preflist)).

%% @doc Testing convert key
convert_key_test()->
    ?assertEqual(1, convert_key(1)),
    ?assertEqual(1, convert_key(-1)),
    ?assertEqual(0, convert_key(0)),
    ?assertEqual(45, convert_key(<<"45">>)),
    ?assertEqual(45, convert_key(<<"-45">>)).

-endif.
