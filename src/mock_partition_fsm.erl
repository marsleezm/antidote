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

-include("antidote.hrl").

%% API
-export([start_link/0]).

%% Callbacks
-export([init/1,
         execute_op/3,
         execute_op/2,
         code_change/4,
         handle_event/3,
         handle_info/3,
         handle_sync_event/4,
         terminate/3]).

-export([get_my_dc_id/0, 
        get_clock_of_dc/2, 
        set_clock_of_dc/3,
        get_preflist_from_key/1,
        read_data_item/4,
        async_read_data_item/4,
        generate_downstream_op/5,
        update_data_item/5,
        prepare/2,
        single_commit/2,
        value/1,
        abort/2,
        commit/3,
        get_stable_snapshot/0
        ]).

-record(state, {
                key :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
gen_fsm:start_link(?MODULE, [], []).

%% @doc Initialize the state.
init([]) ->
{ok, execute_op, #state{}}.

%% Functions that always return the same value no matter the input.
get_my_dc_id() ->
mock_dc.

value(_) ->
mock_value.

get_clock_of_dc(_DcId, _SnapshotTime) ->
{ok, 0}.

set_clock_of_dc(_DcId, _CommitTime, _VecSnapshotTime) ->
dict:new().

get_preflist_from_key(_Key) ->
    {ok, Pid} = mock_partition_fsm:start_link(),
    [Pid].

get_stable_snapshot() ->
    {ok, dict:new()}.

abort(_UpdatedPartitions, _Transactions) ->
    ok.


%% Functions that will return different value depending on Key.
read_data_item(_IndexNode, Key, _Type, TxId) ->
    case Key of 
        read_fail ->
            {error, mock_read_fail};
        counter ->
            Counter = riak_dt_gcounter:new(),
            {ok, Counter1} = riak_dt_gcounter:update(increment, haha, Counter),
            {ok, Counter2} = riak_dt_gcounter:update(increment, nono, Counter1),
            {ok, Counter2};
        {counter, _} ->
            Counter = riak_dt_gcounter:new(),
            {ok, Counter1} = riak_dt_gcounter:update(increment, haha, Counter),
            {ok, Counter2} = riak_dt_gcounter:update(increment, nono, Counter1),
            {ok, Counter2};
        {specula, _, Delay} -> %% Assume this txn depends on some other transaction
            Counter = riak_dt_gcounter:new(),
            {ok, Counter1} = riak_dt_gcounter:update(increment, haha, Counter),
            {ok, Counter2} = riak_dt_gcounter:update(increment, nono, Counter1),
            Sender = self(),
            spawn(fun() -> timer:sleep(Delay), 
                    gen_fsm:send_event(Sender, {read_valid, TxId, 0}) end),
            {specula, Counter2};
        set ->
            Set = riak_dt_gset:new(),
            {ok, Set1} = riak_dt_gset:update({add, a}, haha, Set),
            {ok, Set1}; 
        {set, _} ->
            Set = riak_dt_gset:new(),
            {ok, Set1} = riak_dt_gset:update({add, a}, haha, Set),
            {ok, Set1}; 
        _ ->
            {ok, mock_value}
    end.

generate_downstream_op(_Transaction, _IndexNode, Key, _Type, _Param) ->
    case Key of 
        downstream_fail ->
            {error, mock_downstream_fail};
        _ ->
            {ok, mock_downsteam}
    end.

update_data_item(FsmRef, _Transaction, Key, _Type, _DownstreamRecord) ->
    gen_fsm:sync_send_event(FsmRef, {update_data_item, Key}).

async_read_data_item(FsmRef, Key, _Type, _Transaction) ->
    Self = self(),
    gen_fsm:send_event(FsmRef, {async_read_data_item, Key, Self}).

single_commit([{Node,WriteSet}], TxId) ->
    Self = self(),
    gen_fsm:send_event(Node, {single_commit, {Self, TxId, WriteSet}}).

commit(UpdatedPartitions, _Transaction, _CommitTime) ->
    Self = self(),
    dict:fold(fun(Fsm, _WriteSet, _) -> gen_fsm:send_event(Fsm, {commit, Self}), 
        ok end, [], UpdatedPartitions).

prepare(UpdatedPartitions, TxId) ->
    Self = self(),
    dict:fold(fun(Fsm, WriteSet, _) -> gen_fsm:send_event(Fsm, {prepare, TxId, Self, WriteSet}), 
        ok end, [], UpdatedPartitions).

%% We spawn a new mock_partition_fsm for each update request, therefore
%% a mock fsm will only receive a single update so only need to store a 
%% single updated key. In contrast, clocksi_vnode may receive multiple
%% update request for a single transaction.
execute_op({update_data_item, Key}, _From, State) ->
    case Key of 
        fail_update ->
            {reply, {error, mock_downstream_fail}, execute_op, State#state{key=Key}};
        _ ->
            {reply, ok, execute_op, State#state{key=Key}}
    end.

execute_op({async_read_data_item, Key, From}, State) ->
    {ok,Value} = read_data_item(nothig, no, Key, no),
    gen_fsm:send_event(From, {ok, Value}),
    {next_state, execute_op, State#state{key=Key}};

execute_op({prepare, TxId, From, [{Key, _, _}]}, State) ->
    Now = clocksi_vnode:now_microsec(now()),
    case Key of 
        timeout -> gen_fsm:send_event(From, timeout);
        {wait, Delay} -> timer:sleep(Delay), gen_fsm:send_event(From, {prepared, TxId, Now});
        {abort, Delay} -> timer:sleep(Delay), gen_fsm:send_event(From, {abort, TxId});
        _ -> gen_fsm:send_event(From, {prepared, TxId, Now})
    end,
    {next_state, execute_op, State#state{key=Key}};

execute_op({commit, _From}, State) ->
    {stop, normal, State};

execute_op({single_commit, {From, _TxId, WriteSet}}, State) ->
    [{Key, _, _}] = WriteSet,
    Result = case Key of 
                success -> {committed, 10};
                _ -> abort 
            end,
    gen_fsm:send_event(From, Result),
    {stop, normal, State}.
%% =====================================================================
handle_info(_Info, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(stop,_From,_StateName, StateData) ->
    {stop,normal,ok, StateData};

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

