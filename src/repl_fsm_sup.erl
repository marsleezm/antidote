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

-module(repl_fsm_sup).
-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).

-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_fsm(Partition) ->
    supervisor:start_child(?MODULE, [Partition]).

generate_data_repl_serv() ->
    ToReplicate = find_to_repl(),
    Names = [ list_to_atom(atom_to_list(node())++"repl"++atom_to_list(Node))  || Node<- ToReplicate],
    [{Name, {data_repl_serv, start_link, [Name]},
        permanent, 5000, worker, [data_repl_serv]}
            || Name <- Names ].

find_to_repl() ->
    List = antidote_config:get(to_repl),
    [{_, ToRepl}] = lists:filter(fun({Node, _}) -> Node == node() end, List),
    ToRepl. 

init([]) ->
    case antidote_config:get(do_repl) of
        true ->
            MyRepFsm = {repl_fsm, {repl_fsm, start_link, []}, transient, 5000, worker, [repl_fsm]},
            DataReplFsms = generate_data_repl_serv(), 
            CacheServ = {cache_serv, {cache_serv, start_link, []}, transient, 5000, worker, [cache_serv]}, 
            {ok, {{one_for_one, 5, 10}, [CacheServ|[MyRepFsm|DataReplFsms]]}};
        false ->
            {ok, {{one_for_one, 5, 10}, []}}
    end.
