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

-export([confirm/0, clocksi_hash_fun_test/1, local_hash_fun/2, get_part/3]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    clocksi_hash_fun_test(Nodes),
    rt:clean_cluster(Nodes),
    pass.

clocksi_hash_fun_test(Nodes) ->
    GetHashFun = fun(Node, Acc) -> [rpc:call(Node, hash_fun, get_hash_fun,
                                    []) | Acc]
                 end , 
    AllHashFun = lists:foldl(GetHashFun, [], Nodes),
    FirstFun = hd(AllHashFun),
    lists:foreach(fun(X) -> ?assertEqual(X, FirstFun) end, AllHashFun).

local_hash_fun(Key, PartList) ->
    TotalLength = lists:foldl(fun({_, Num}, Acc) ->  Num+Acc end, PartList),
    Remain = Key rem TotalLength,
    get_part(Remain, PartList, 1).

get_part(Remain, [{_, Num}|Rest], Acc) -> 
    case Remain > Num of 
        true ->
            get_part(Remain - Num, Rest, Acc+1);
        false ->
            {Acc, Remain-Num+1}
    end. 

