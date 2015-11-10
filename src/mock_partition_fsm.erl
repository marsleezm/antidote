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
%% @doc A mocked file that emulates the behavior of several antidote 
%%      components which relies on riak-core backend, e.g. 
%%      clocksi_vnode, dc_utilities and log_utilities. For simplicity,
%%      the reply of some functions depend on the key being updated.
%%      The detailed usage can be checked within each function, which is
%%      self-explanatory.

-module(mock_partition_fsm).

-behavior(gen_server).

-include("antidote.hrl").

%% API
-export([start_link/0]).

%% Callbacks
-export([init/1,
        handle_call/3,
        handle_cast/2,
        code_change/3,
        handle_event/3,
        handle_info/2,
        handle_sync_event/4,
        terminate/2]).

-export([
        repl_commit/4,
        repl_abort/3,
        abort/2,
        commit/3,
        if_applied/4,
        go_down/0
        ]).

-record(state, {table :: dict(),
                key :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, test_fsm},
             ?MODULE, [], []).

%% @doc Initialize the state.
init([]) ->
    {ok, #state{table=dict:new()}}.

if_applied(UpdatedParts, TxId, Result, Type) ->
    gen_server:call(test_fsm, {if_applied, Type, TxId, UpdatedParts, Result}).

repl_commit(UpdatedParts, TxId, CommitTime, true) ->
    gen_server:cast(test_fsm, {repl_commit, TxId, UpdatedParts, CommitTime}).

repl_abort(UpdatedParts, TxId, true) ->
    gen_server:cast(test_fsm, {repl_abort, TxId, UpdatedParts}).

abort(UpdatedParts, TxId) ->
    gen_server:cast(test_fsm, {abort, TxId, UpdatedParts}).

commit(UpdatedParts, TxId, CommitTime) ->
    gen_server:cast(test_fsm, {commit, TxId, UpdatedParts, CommitTime}).

go_down() ->
    gen_server:call(test_fsm, {go_down}).


handle_call({if_applied, Type, TxId, Parts, Result}, _Sender, State=#state{table=Table}) ->
    io:format(user, "if applied ~w, ~w, ~w ~n", [Type, TxId, Parts]),
    {ok, Reply} = dict:find({Type, Parts, TxId}, Table),
    {reply, (Reply == Result), State};

handle_call({go_down},_Sender,SD0) ->
    %io:format(user, "shutting down machine ~w", [self()]),
    {stop,shutdown,ok,SD0}.

handle_cast({repl_commit, TxId, Parts, CommitTime}, State=#state{table=Table}) ->
    {noreply, State#state{table=dict:store({repl_commit, Parts, TxId}, CommitTime, Table)}};

handle_cast({repl_abort, TxId, Parts}, State=#state{table=Table}) ->
    %io:format(user, "rebar_abort ~w ~w ~n", [TxId, Parts]),
    {noreply, State#state{table=dict:store({repl_abort, Parts, TxId}, nothing, Table)}};

handle_cast({abort, TxId, Parts}, State=#state{table=Table}) ->
    %io:format(user, "abort ~w ~w ~n", [TxId, Parts]),
    {noreply, State#state{table=dict:store({abort, Parts, TxId}, nothing, Table)}};

handle_cast({commit, TxId, Parts, CommitTime}, State=#state{table=Table}) ->
    {noreply, State#state{table=dict:store({commit, Parts, TxId}, CommitTime, Table)}};

handle_cast(_Info, StateData) ->
    {noreply,StateData}.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(Reason, _SD) ->
    lager:info("Cert server terminated with ~w", [Reason]),
    ok.
