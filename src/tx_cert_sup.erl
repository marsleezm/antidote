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

-module(tx_cert_sup).

-behavior(supervisor).

-include("antidote.hrl").

-export([start_link/0]).

-export([init/1, certify/4, get_stat/0, get_internal_data/3, start_tx/1, 
            set_internal_data/3, read/4, single_commit/4]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

single_commit(Name, Node, Key, Value) ->
    case is_integer(Name) of
        true ->
            gen_server:call({global, generate_module_name(Name rem ?NUM_SUP)}, 
                    {single_commit, Node, Key, Value});
        false ->
            gen_server:call({global, Name}, 
                    {single_commit, Node, Key, Value})
    end.

start_tx(Name) ->
    case is_integer(Name) of
        true ->
            gen_server:call({global, generate_module_name(Name rem ?NUM_SUP)}, {start_tx});
        false ->
            gen_server:call({global, Name}, {start_tx})
    end.

certify(Name, TxId, LocalUpdates, RemoteUpdates) ->
    case is_integer(Name) of
        true ->
            gen_server:call({global, generate_module_name(Name rem ?NUM_SUP)}, 
                    {certify, TxId, LocalUpdates, RemoteUpdates});
        false ->
            gen_server:call({global, Name}, 
                    {certify, TxId, LocalUpdates, RemoteUpdates})
    end.

get_internal_data(Name, Type, Param) ->
    case is_integer(Name) of
        true ->
            gen_server:call({global, generate_module_name(Name rem ?NUM_SUP)}, {get_internal_data, Type, Param});
        false ->
            gen_server:call({global, Name}, {get_internal_data, Type, Param})
    end.

set_internal_data(Name, Type, Param) ->
    case is_integer(Name) of
        true ->
            gen_server:call({global, generate_module_name(Name rem ?NUM_SUP)}, {set_internal_data, Type, Param});
        false ->
            gen_server:call({global, Name}, {set_internal_data, Type, Param})
    end.

read(Name, TxId, Key, Node) ->
    case is_integer(Name) of
        true ->
            gen_server:call({global, generate_module_name(Name rem ?NUM_SUP)}, {read, Key, TxId, Node});
        false ->
            gen_server:call({global, Name}, {read, Key, TxId, Node})
    end.

get_stat() ->
    SPL = lists:seq(1, ?NUM_SUP),
    {R1, R2, R3, R4, SpeculaReadTxns} = lists:foldl(fun(N, {A1, A2, A3, A4, A5}) ->
                            {T1, T2, T3, T4, T5} = gen_server:call({global, generate_module_name(N rem ?NUM_SUP)}, {get_stat}),
                            {A1+T1, A2+T2, A3+T3, A4+T4, A5+T5} end, {0,0,0,0,0}, SPL),
    LocalServ = hash_fun:get_local_servers(),
    PRead = lists:foldl(fun(S, Acc) ->
                        Num = clocksi_vnode:num_specula_read(S), Num+Acc
                        end, 0, LocalServ), 
    Lists = antidote_config:get(to_repl),
    [LocalRepList] = [LocalReps || {Node, LocalReps} <- Lists, Node == node()],
    LocalRepNames = [list_to_atom(atom_to_list(node())++"repl"++atom_to_list(L))  || L <- LocalRepList ],
    {DSpeculaRead, DTotalRead} = lists:foldl(fun(S, {Acc1, Acc2}) ->
                        {Num1, Num2} = data_repl_serv:num_specula_read(S), {Num1+Acc1, Num2+Acc2}
                        end, {0, 0}, LocalRepNames), 
    lager:info("Data replica specula read is ~w, Data replica read is ~w", [DSpeculaRead, DTotalRead]),
    {CacheSpeculaRead, CacheAttemptRead} = cache_serv:num_specula_read(),
    {R1, R2, R3, R4, SpeculaReadTxns, PRead,  
        DSpeculaRead, DTotalRead, CacheSpeculaRead, CacheAttemptRead}.

generate_module_name(N) ->
    list_to_atom(atom_to_list(node()) ++ "-cert-" ++ integer_to_list(N)).

generate_supervisor_spec(N) ->
    Module = generate_module_name(N),
    case ets:lookup(meta_info, do_specula) of
        [{do_specula, true}] ->
            {Module,
             {specula_tx_cert_server, start_link, [Module]},
              permanent, 5000, worker, [specula_tx_cert_server]};
        [{do_specula, false}] ->
            {Module,
             {i_tx_cert_server, start_link, [Module]},
              permanent, 5000, worker, [i_tx_cert_server]}
    end.

%% @doc Starts the coordinator of a ClockSI interactive transaction.
init([]) ->
    Pool = [generate_supervisor_spec(N) || N <- lists:seq(0, ?NUM_SUP-1)],
    {ok, {{one_for_one, 5, 10}, Pool}}.
