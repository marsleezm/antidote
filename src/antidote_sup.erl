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
-module(antidote_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_rep/2, stop_rep/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc: start_rep(Port) - starts a server managed by Pid which listens for 
%% incomming tcp connection on port Port. Server receives updates to replicate 
%% from other DCs 
start_rep(Pid, Port) ->
    supervisor:start_child(?MODULE, {inter_dc_communication_sup,
                    {inter_dc_communication_sup, start_link, [Pid, Port]},
                    permanent, 5000, supervisor, [inter_dc_communication_sup]}).

stop_rep() ->
    ok = supervisor:terminate_child(inter_dc_communication_sup, inter_dc_communication_recvr),
    _ = supervisor:delete_child(inter_dc_communication_sup, inter_dc_communication_recvr),
    ok = supervisor:terminate_child(inter_dc_communication_sup, inter_dc_communication_fsm_sup),
    _ = supervisor:delete_child(inter_dc_communication_sup, inter_dc_communication_fsm_sup),
    ok = supervisor:terminate_child(?MODULE, inter_dc_communication_sup),
    _ = supervisor:delete_child(?MODULE, inter_dc_communication_sup),
    ok.
    
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    antidote_config:load("antidote.config"),
    ets:new(meta_info,
        [set,public,named_table,{read_concurrency,true},{write_concurrency,false}]),
    lager:info("Cdf started!"),
    ets:new(dependency,
        [bag,public,named_table,{read_concurrency,true},{write_concurrency,true}]),
    ets:new(anti_dep,
        [bag,public,named_table,{read_concurrency,true},{write_concurrency,true}]),

         case antidote_config:get(do_specula) of
            true -> 
                ets:insert(meta_info, {do_specula, true});
                   % { specula_general_tx_coord_sup,
                   %         {specula_general_tx_coord_sup, start_link, []},
                   %         permanent, 5000, supervisor, [specula_general_tx_coord_sup]}
            false ->
                ets:insert(meta_info, {do_specula, false})
                  %{ clocksi_general_tx_coord_sup,
                  %          {clocksi_general_tx_coord_sup, start_link, []},
                  %          permanent, 5000, supervisor, [clockSI_general_tx_coord_sup]}
          end,

    VnodeMaster = { clocksi_vnode_master,
                        {riak_core_vnode_master, start_link, [clocksi_vnode]},
                        permanent, 5000, worker, [riak_core_vnode_master]}, 
    
    ReplFsmSup = {repl_fsm_sup,
                  {repl_fsm_sup, start_link, []},
                  permanent, 5000, supervisor,
                  [repl_fsm_sup]},

    StatServer = {stat_server,
                    {stat_server,  start_link,
                    []},
                    permanent, 5000, worker, [stat_server]},

    CertSup = {tx_cert_sup,
                    {tx_cert_sup,  start_link, []},
                    permanent, 5000, worker, [tx_cert_sup]},


    {ok,
     {{one_for_one, 5, 10},
      [VnodeMaster,
       ReplFsmSup,
       CertSup,
       StatServer]}}.
