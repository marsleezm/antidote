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
%% TODO: should implement heart-beat for timestamp.
-module(cache_serv).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NUM_VERSIONS, 10).
%% API
-export([start_link/1]).

-define(CLOCKSI_VNODE, clocksi_vnode).

%% Callbacks
-export([init/1,
	    handle_call/3,
	    handle_cast/2,
        code_change/3,
        handle_event/3,
        handle_info/2,
        handle_sync_event/4,
        terminate/2]).

%% States
-export([
        prepare_specula/4,
        commit/3,
        pre_commit/3,
        abort/2,
        if_prepared/2,
        num_specula_read/0,
        local_certify/3,
        read/4,
        read/2,
        read/3]).

%% Spawn

-record(state, {
        prepared_txs :: cache_id(),
        dep_dict :: dict(),
        delay :: non_neg_integer(),
        specula_read :: boolean(),
        num_specula_read :: non_neg_integer(),
        num_attempt_read :: non_neg_integer(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name) ->
    gen_server:start_link({local, Name},
             ?MODULE, [], []).

read(Name, Key, TxId, Node) ->
    gen_server:call(Name, {read, Key, TxId, Node}).

read(Key, TxId, Node) ->
    gen_server:call(node(), {read, Key, TxId, Node}).

read(Key, TxId) ->
    gen_server:call(node(), {read, Key, TxId}).

num_specula_read() ->
    gen_server:call(node(), {num_specula_read}).

if_prepared(TxId, Keys) ->
    gen_server:call(node(), {if_prepared, TxId, Keys}).

prepare_specula(TxId, Partition, WriteSet, PrepareTime) ->
    gen_server:cast(node(), {prepare_specula, TxId, Partition, WriteSet, PrepareTime}).

abort(TxId, Partition) -> 
    gen_server:cast(node(), {abort, TxId, Partition}).

commit(TxId, Partition, CommitTime) -> 
    gen_server:cast(node(), {commit, TxId, Partition, CommitTime}).

local_certify(TxId, Partition, WriteSet) ->
    gen_server:cast(node(), {local_certify, TxId, Partition, WriteSet, self()}).

pre_commit(TxId, Partition, SpeculaCommitTs) ->
    gen_server:cast(node(), {pre_commit, TxId, Partition, SpeculaCommitTs}).

%%%===================================================================
%%% Internal
%%%===================================================================

init([]) ->
    lager:info("Cache server inited"),
    PreparedTxs = tx_utilities:open_private_table(prepared_txs),
    SpeculaRead = antidote_config:get(specula_read),
    {ok, #state{specula_read = SpeculaRead, dep_dict=dict:new(),
                prepared_txs = PreparedTxs, num_specula_read=0, num_attempt_read=0}}.

handle_call({num_specula_read}, _Sender, 
	    SD0=#state{num_specula_read=NumSpeculaRead, num_attempt_read=NumAttemptRead}) ->
    {reply, {NumSpeculaRead, NumAttemptRead}, SD0};

handle_call({get_pid}, _Sender, SD0) ->
        {reply, self(), SD0};

handle_call({clean_data}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    ets:delete_all_objects(PreparedTxs),
    {reply, ok, SD0#state{num_specula_read=0, num_attempt_read=0}};

handle_call({read, Key, TxId, Node}, Sender, 
	    SD0=#state{specula_read=false}) ->
    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, false),
    {noreply, SD0};

handle_call({read, Key, TxId, {Partition, _}=Node, SpeculaRead}, Sender,
        SD0=#state{prepared_txs=PreparedTxs}) ->
    case SpeculaRead of
        false ->
            ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, false),
            {noreply, SD0};
        true ->
            %lager:warning("Cache specula read ~w of ~w", [Key, TxId]), 
            case local_cert_util:specula_read(TxId, {Partition, Key}, PreparedTxs, {TxId, Node, {relay, Sender}}) of
                not_ready->
                    %lager:warning("Read blocked!"),
                    {noreply, SD0};
                {specula, Value} ->
                    {reply, {ok, Value}, SD0};
                ready ->
                    %lager:warning("Read finished!"),
                    ?CLOCKSI_VNODE:relay_read(Node, Key, TxId, Sender, false),
                    {noreply, SD0}
            end
    end;

handle_call({read, Key, TxId}, _Sender,
        SD0=#state{prepared_txs=PreparedTxs, specula_read=false}) ->
    case ets:lookup(PreparedTxs, Key) of
        [] ->
            {reply, {ok, []}, SD0};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            case find_version(ValueList, MyClock) of
                {SpeculaTxId, Value} ->
                    ets:insert(dependency, {SpeculaTxId, TxId}),
                    %lager:info("Inserting anti_dep from ~w to ~w for ~p", [TxId, SpeculaTxId, Key]),
                    ets:insert(anti_dep, {TxId, SpeculaTxId}),
                    {reply, {ok, Value}, SD0};
                [] ->
                    {reply, {ok, []}, SD0}
            end
    end;

handle_call({if_prepared, TxId, Keys}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    Result = helper:handle_if_prepared(TxId, Keys, PreparedTxs),
    {reply, Result, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({prepare_specula, TxId, Partition, WriteSet, TimeStamp}, 
	    SD0=#state{prepared_txs=PreparedTxs}) ->
    KeySet = lists:foldl(fun({Key, Value}, KS) ->
                    PartKey = {Partition, Key},
                    case ets:lookup(PreparedTxs, PartKey) of
                        [] ->
                            %% Putting TxId in the record to mark the transaction as speculative 
                            %% and for dependency tracking that will happen later
                            true = ets:insert(PreparedTxs, {PartKey, [{TimeStamp, Value, TxId}]}),
                            [Key|KS];
                        [{PartKey, ValueList}] ->
                            {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                            true = ets:insert(PreparedTxs, {PartKey, [{TimeStamp, Value, TxId}|RemainList]}),
                            [Key|KS]
                    end end, [], WriteSet),
    ets:insert(PreparedTxs, {{TxId, Partition}, KeySet}),
    {noreply, SD0};

%% Need to certify here
handle_cast({local_certify, TxId, Partition, WriteSet, Sender},
        SD0=#state{prepared_txs=PreparedTxs, dep_dict=DepDict}) ->
   %lager:warning("cache server local certify for [~w, ~w]", [TxId, Partition]),
    Result = local_cert_util:prepare_for_other_part(TxId, Partition, WriteSet, ignore, PreparedTxs, TxId#tx_id.snapshot_time, cache),
    case Result of
        {ok, PrepareTime} ->
            %UsedTime = tx_utilities:now_microsec() - PrepareTime,
            %lager:warning("~w: ~w certification check prepred with ~w", [Partition, TxId, PrepareTime]),
            gen_server:cast(Sender, {prepared, TxId, PrepareTime}),
            {noreply, SD0};
        {wait, PendPrepDep, _PrepDep, PrepareTime} ->
            NewDepDict =
                case PendPrepDep of
                    0 -> gen_server:cast(Sender, {prepared, TxId, PrepareTime}), DepDict;
                    _ -> dict:store(TxId, {PendPrepDep, PrepareTime, Sender}, DepDict)
                end,
            {noreply, SD0#state{dep_dict=NewDepDict}};
        {error, write_conflict} ->
            gen_server:cast(Sender, {aborted, TxId}),
            {noreply, SD0}
    end;

handle_cast({pre_commit, TxId, Partition, SpeculaCommitTime}, State=#state{prepared_txs=PreparedTxs,
        dep_dict=DepDict}) ->
   %lager:warning("specula commit for [~w, ~w]", [TxId, Partition]),
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
        [{{TxId, Partition}, Keys}] ->
            DepDict1 = local_cert_util:pre_commit(Keys, TxId, SpeculaCommitTime, ignore, PreparedTxs, DepDict, Partition, cache),
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
            lager:error("Prepared record of ~w has disappeared!", [TxId]),
            error
    end;

%% Where shall I put the speculative version?
%% In ets, faster for read.
handle_cast({abort, TxId, Partitions}, 
	    SD0=#state{prepared_txs=PreparedTxs, dep_dict=DepDict}) ->
   %lager:warning("Abort ~w in cache", [TxId]),
    DepDict1 = lists:foldl(fun(Partition, D) ->
            case ets:lookup(PreparedTxs, {TxId, Partition}) of 
                [{{TxId, Partition}, KeySet}] -> 
                    ets:delete(PreparedTxs, {TxId, Partition}),
                    local_cert_util:clean_abort_prepared(PreparedTxs, KeySet, TxId, ignore, D, Partition, cache);
                _ -> D 
            end end, DepDict, Partitions),
    specula_utilities:deal_abort_deps(TxId),
    {noreply, SD0#state{dep_dict=DepDict1}};
    
handle_cast({commit, TxId, Partitions, CommitTime}, 
	    SD0=#state{prepared_txs=PreparedTxs, dep_dict=DepDict}) ->
   %lager:warning("Commit ~w in cache", [TxId]),
    DepDict1 = lists:foldl(fun(Partition, D) ->
            case ets:lookup(PreparedTxs, {TxId, Partition}) of
                [{{TxId, Partition}, KeySet}] ->
                    ets:delete(PreparedTxs, {TxId, Partition}),
                    local_cert_util:update_store(KeySet, TxId, CommitTime, ignore, ignore, PreparedTxs,
                        D, Partition, cache);
                _ ->
                    D
            end end, DepDict, Partitions),
    specula_utilities:deal_commit_deps(TxId, CommitTime),
    {noreply, SD0#state{dep_dict=DepDict1}};

handle_cast(_Info, StateData) ->
    {noreply,StateData}.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

find_version([],  _SnapshotTime) ->
    [];
find_version([{TS, Value, TxId}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {TxId, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.

