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
-module(locating_test).

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
    lager:info("Vnodes are started up"),

    rt:log_to_nodes(Nodes, "Starting write operation 1"),

    [First|_]=Nodes,
    Key = 100,
    [{Part,Node}] = rpc:call(First, log_utilities, get_preflist_from_key, [Key]),
    Next = rpc:call(First, log_utilities, get_my_next, [Part, 3]),

    {ok, CHBin} = rpc:call(First, riak_core_ring_manager, get_chash_bin, []),
    PartitionList = chashbin:to_list(CHBin),
    MyNext = get_my_next(PartitionList, {Part,Node}, 3),
    ?assertEqual(Next, MyNext),


    pass.


get_my_next(List, Part, Num) ->
    Index = index_of(Part, List, 1),
    Length = length(List),
    case Index + Num  > Length of
        true ->
            FirstPart = Length - Index,
            A = lists:sublist(List, Index+1, FirstPart), 
            B = lists:sublist(List, 1, Num - FirstPart),
            A ++ B;
        false ->
            lists:sublist(List, Index+1, Num)
    end.

index_of(_, [], _)  -> not_found;
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index) -> index_of(Item, Tl, Index+1).
    
