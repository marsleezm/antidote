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
        repl_commit/6,
        repl_commit/5,
        repl_commit/3,
        repl_abort/5,
        repl_abort/4,
        repl_abort/2,
        add_dependency/3,
        abort/2,
        commit/3,
        read_valid/3,
        read_invalid/3,
        if_applied/2,
        specula_prepare/5,
        specula_prepare/4,
        %commit_specula/4,
        %commit_specula/3,
        %abort_specula/3,
        %abort_specula/2,
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

if_applied(Param, Result) ->
    gen_server:call(test_fsm, {if_applied, Param, Result}).

repl_commit(UpdatedParts, TxId, CommitTime) ->
    gen_server:cast(test_fsm, {repl_commit, TxId, UpdatedParts, CommitTime}).

repl_commit(UpdatedParts, TxId, CommitTime, _, _,_) ->
    gen_server:cast(test_fsm, {repl_commit, TxId, UpdatedParts, CommitTime}).

repl_commit(UpdatedParts, TxId, CommitTime, _, _) ->
    gen_server:cast(test_fsm, {repl_commit, TxId, UpdatedParts, CommitTime}).

repl_abort(UpdatedParts, TxId) ->
    gen_server:cast(test_fsm, {repl_abort, TxId, UpdatedParts}).

add_dependency(WriteTxId, ReadTxId, NumDeps) ->
    gen_server:cast(test_fsm, {add_dependency, WriteTxId, ReadTxId, NumDeps}).

repl_abort(UpdatedParts, TxId, _, _,_) ->
    gen_server:cast(test_fsm, {repl_abort, TxId, UpdatedParts}).

repl_abort(UpdatedParts, TxId, _, _) ->
    gen_server:cast(test_fsm, {repl_abort, TxId, UpdatedParts}).

read_valid(_, RTxId, WTxId) ->
    gen_server:cast(test_fsm, {read_valid, RTxId, WTxId}).

read_invalid(_, _, TxId) ->
    gen_server:cast(test_fsm, {read_invalid, TxId}).

abort(UpdatedParts, TxId) ->
    gen_server:cast(test_fsm, {abort, TxId, UpdatedParts}).

commit(UpdatedParts, TxId, CommitTime) ->
    gen_server:cast(test_fsm, {commit, TxId, UpdatedParts, CommitTime}).

specula_prepare(DataReplServ, TxId, Partition, Updates, PrepareTime) ->
    gen_server:cast(test_fsm, {specula_prepare, DataReplServ, TxId, Partition, Updates, PrepareTime}).

specula_prepare(TxId, Partition, Updates, PrepareTime) ->
    gen_server:cast(test_fsm, {specula_prepare, cache_serv, TxId, Partition, Updates, PrepareTime}).

%commit_specula(DataReplServ, TxId, Partition, CommitTime) ->
%    gen_server:cast(test_fsm, {commit_specula, DataReplServ, TxId, Partition, CommitTime}).

%commit_specula(TxId, Partition, CommitTime) ->
%    gen_server:cast(test_fsm, {commit_specula, cache_serv, TxId, Partition, CommitTime}).

%abort_specula(DataReplServ, TxId, Partition) ->
%    gen_server:cast(test_fsm, {abort_specula, DataReplServ, TxId, Partition}).

%abort_specula(TxId, Partition) ->
%    gen_server:cast(test_fsm, {abort_specula, cache_serv, TxId, Partition}).

go_down() ->
    gen_server:call(test_fsm, {go_down}).


handle_call({if_applied, Param, Result}, _Sender, State=#state{table=Table}) ->
    io:format(user, "if applied ~w, result is ~w ~n", [Param, Result]),
    {ok, Reply} = dict:find(Param, Table),
    {reply, (Reply == Result), State};

handle_call({go_down},_Sender,SD0) ->
    %io:format(user, "shutting down machine ~w", [self()]),
    {stop,shutdown,ok,SD0}.

handle_cast({specula_prepare, ServName, TxId, Partition, Updates, PrepareTime}, State=#state{table=Table}) ->
    {noreply, State#state{table=dict:store({specula_prepare, ServName, TxId, Partition}, {Updates, PrepareTime}, Table)}};

handle_cast({commit_specula, ServName, TxId, Partition, CommitTime}, State=#state{table=Table}) ->
    io:format(user, "commit_specula ~w ~w ~w ~w ~n", [ServName, TxId, Partition, CommitTime]),
    {noreply, State#state{table=dict:store({commit_specula, ServName, TxId, Partition}, CommitTime, Table)}};

handle_cast({abort_specula, ServName, TxId, Partition}, State=#state{table=Table}) ->
    io:format(user, "abort_specula ~w ~w ~w ~n", [ServName, TxId, Partition]),
    {noreply, State#state{table=dict:store({abort_specula, ServName, TxId, Partition}, nothing, Table)}};

handle_cast({read_valid, RTxId, WTxId}, State=#state{table=Table}) ->
    {noreply, State#state{table=dict:store({read_valid, RTxId}, WTxId, Table)}};

handle_cast({read_invalid, TxId}, State=#state{table=Table}) ->
    {noreply, State#state{table=dict:store({read_invalid, TxId}, nothing, Table)}};

handle_cast({add_dependency, WriteTxId, ReadTxId, NumDep}, State=#state{table=Table}) ->
    case dict:find({dep, WriteTxId}, Table) of
        error -> {noreply, State#state{table=dict:store({dep, WriteTxId}, [{ReadTxId, NumDep}], Table)}};
        {ok, List} ->  {noreply, State#state{table=dict:store({dep, WriteTxId}, [{ReadTxId, NumDep}|List], Table)}}
    end;

handle_cast({repl_commit, TxId, Parts, CommitTime}, State=#state{table=Table}) ->
    io:format(user, "repl_commit ~w ~w  ~w ~n", [TxId, Parts, CommitTime]),
    case dict:find({dep, TxId}, Table) of
        error -> ok;
        {ok, List} -> 
            lists:foreach(fun({ReadTxId, DepNum}) ->
                            case ReadTxId#tx_id.snapshot_time >= CommitTime of 
                                true ->
                                    gen_server:cast(ReadTxId#tx_id.server_pid, {read_valid, ReadTxId, DepNum});
                                false ->
                                    gen_server:cast(ReadTxId#tx_id.server_pid, {read_invalid, CommitTime, ReadTxId})
                            end end, List)
    end,
    Table1 = dict:erase({dep, TxId}, Table),
    {noreply, State#state{table=dict:store({repl_commit, TxId, Parts}, CommitTime, Table1)}};

handle_cast({repl_abort, TxId, Parts}, State=#state{table=Table}) ->
    io:format(user, "rebar_abort ~w ~w ~n", [TxId, Parts]),
    case dict:find({dep, TxId}, Table) of
        error -> ok;
        {ok, List} -> 
            lists:foreach(fun({ReadTxId, _DepNum}) ->
                            gen_server:cast(ReadTxId#tx_id.server_pid, {read_aborted, -1, ReadTxId})
                            end, List)
    end,
    Table1 = dict:erase({dep, TxId}, Table),
    {noreply, State#state{table=dict:store({repl_abort, TxId, Parts}, nothing, Table1)}};

handle_cast({abort, TxId, Parts}, State=#state{table=Table}) ->
    io:format(user, "abort ~w ~w ~n", [TxId, Parts]),
    case dict:find({dep, TxId}, Table) of
        error -> ok;
        {ok, List} -> 
            lists:foreach(fun({ReadTxId, _DepNum}) ->
                            gen_server:cast(ReadTxId#tx_id.server_pid, {read_aborted, -1, ReadTxId})
                            end, List)
    end,
    Table1 = dict:erase({dep, TxId}, Table),
    {noreply, State#state{table=dict:store({abort, TxId, Parts}, nothing, Table1)}};

handle_cast({commit, TxId, Parts, CommitTime}, State=#state{table=Table}) ->
    case dict:find({dep, TxId}, Table) of
        error -> ok;
        {ok, List} -> 
            lists:foreach(fun({ReadTxId, DepNum}) ->
                            case ReadTxId#tx_id.snapshot_time >= CommitTime of 
                                true ->
                                    gen_server:cast(ReadTxId#tx_id.server_pid, {read_valid, ReadTxId, DepNum});
                                false ->
                                    gen_server:cast(ReadTxId#tx_id.server_pid, {read_invalid, CommitTime, ReadTxId})
                            end end, List)
    end,
    Table1 = dict:erase({dep, TxId}, Table),
    {noreply, State#state{table=dict:store({commit, TxId, Parts}, CommitTime, Table1)}};

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
