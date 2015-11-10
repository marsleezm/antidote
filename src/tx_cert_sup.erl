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

-export([init/1, certify/4, get_stat/1, get_internal_data/3, read/4]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

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

read(Name, TxId, Key, Node) ->
    case is_integer(Name) of
        true ->
            gen_server:call({global, generate_module_name(Name rem ?NUM_SUP)}, {read, Key, TxId, Node});
        false ->
            gen_server:call({global, Name}, {read, Key, TxId, Node})
    end.

get_stat(Name) ->
    case is_integer(Name) of
        true ->
            gen_server:call({global, generate_module_name(Name rem ?NUM_SUP)}, {get_stat});
        false ->
            gen_server:call({global, Name}, {get_stat})
    end.

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
