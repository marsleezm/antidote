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
-module(clocksi_readitem_fsm).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/2]).

%% Callbacks
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
         code_change/3,
         handle_event/3,
	 check_servers_ready/0,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-export([read_data_item/4,
        async_read_data_item/4,
	 check_partition_ready/3,
	 start_read_servers/1,
	 stop_read_servers/1]).

%% Spawn

-record(state, {partition :: non_neg_integer(),
		id :: non_neg_integer(),
		snapshot_cache :: cache_id(),
        specula_cache :: cache_id(),
        specula_dep :: cache_id(),
		prepared_cache :: cache_id(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Partition,Id) ->
    Addr = node(),
    gen_server:start_link({global,generate_server_name(Addr,Partition,Id)}, ?MODULE, [Partition,Id], []).

start_read_servers(Partition) ->
    Addr = node(),
    start_read_servers_internal(Addr, Partition, ?READ_CONCURRENCY).

stop_read_servers(Partition) ->
    Addr = node(),
    stop_read_servers_internal(Addr, Partition, ?READ_CONCURRENCY).


read_data_item({Partition,Node},Key,Type,TxId) ->
    try
	gen_server:call({global,generate_random_server_name(Node,Partition)},
			{perform_read,Key,Type,TxId},infinity)
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

async_read_data_item({Partition,Node},Key,Type,TxId) ->
    %lager:info("Sending read for ~w",[Key]),
    try
	gen_server:cast({global,generate_random_server_name(Node,Partition)},
			{perform_read_cast, {async,self()}, Key, Type, TxId})
    catch
        _:Reason ->
            lager:error("Exception caught: ~p", [Reason]),
            {error, Reason}
    end.

check_servers_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_server_ready(PartitionList).

check_server_ready([]) ->
    true;
check_server_ready([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_servers_ready},
						 ?CLOCKSI_MASTER,
						 infinity),
    case Result of
	false ->
	    false;
	true ->
	    check_server_ready(Rest)
    end.

check_partition_ready(_Node,_Partition,0) ->
    true;
check_partition_ready(Node,Partition,Num) ->
    case global:whereis_name(generate_server_name(Node,Partition,Num)) of
	undefined ->
	    false;
	_Res ->
	    check_partition_ready(Node,Partition,Num-1)
    end.



%%%===================================================================
%%% Internal
%%%===================================================================

start_read_servers_internal(_Node,_Partition,0) ->
    ok;
start_read_servers_internal(Node, Partition, Num) ->
    {ok,_Id} = clocksi_readitem_sup:start_fsm(Partition,Num),
    start_read_servers_internal(Node, Partition, Num-1).

stop_read_servers_internal(_Node,_Partition,0) ->
    ok;
stop_read_servers_internal(Node,Partition, Num) ->
    try
	gen_server:call({global,generate_server_name(Node,Partition,Num)},{go_down})
    catch
	_:_Reason->
	    ok
    end,
    stop_read_servers_internal(Node, Partition, Num-1).


generate_server_name(Node, Partition, Id) ->
    list_to_atom(integer_to_list(Id) ++ integer_to_list(Partition) ++ atom_to_list(Node)).

generate_random_server_name(Node, Partition) ->
    generate_server_name(Node, Partition, random:uniform(?READ_CONCURRENCY)).

init([Partition, Id]) ->
    Addr = node(),
    SnapshotCache = clocksi_vnode:get_cache_name(Partition,inmemory_store),
    PreparedCache = clocksi_vnode:get_cache_name(Partition, prepared),
    SpeculaCache = clocksi_vnode:get_cache_name(Partition, specula_store),
    SpeculaDep = clocksi_vnode:get_cache_name(Partition, specula_dep),
    Self = generate_server_name(Addr,Partition,Id),
    {ok, #state{partition=Partition, id=Id, 
		snapshot_cache=SnapshotCache,
		specula_cache=SpeculaCache,
        specula_dep=SpeculaDep,
		prepared_cache=PreparedCache,self=Self}}.

handle_call({perform_read, Key, Type, TxId},Coordinator,
	    SD0=#state{self=Self}) ->
    %lager:info("Got read request for ~w", Key),
    perform_read_internal({sync,Coordinator},Key,Type,TxId, 
                        Self, SD0),
    {noreply,SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({perform_read_cast, Coordinator, Key, Type, TxId},
	    SD0=#state{self=Self}) ->
    perform_read_internal(Coordinator,Key,Type,TxId,Self, SD0),
    {noreply,SD0}.

perform_read_internal(Coordinator,Key,Type,TxId,Self,State) ->
    case check_clock(Key,TxId,State) of
	not_ready ->
        %lager:info("Clock not ready"),
	    spin_wait(Coordinator,Key,Type,TxId,Self,State);
	ready ->
        %lager:info("Ready to read for key ~w to ~w",[Key, Coordinator]),
	    return(Coordinator,Key,Type,TxId,State#state.snapshot_cache,State#state.specula_cache)
    end.

spin_wait(Coordinator,Key,Type,TxId,Self,State) ->
    {message_queue_len,Length} = process_info(self(), message_queue_len),
    case Length of
	0 ->
        %lager:info("Sleeping to wati for read ~w",[Key]),
	    timer:sleep(?SPIN_WAIT),
	    perform_read_internal(Coordinator,Key,Type,TxId,Self,State);
	_ ->
        %lager:info("Sending myself to read ~w",[Key]),
	    gen_server:cast({global,Self},{perform_read_cast,Coordinator,Key,Type,TxId})
    end.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behind, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
check_clock(Key,TxId,State) ->
    T_TS = TxId#tx_id.snapshot_time,
    Time = clocksi_vnode:now_microsec(erlang:now()),
    case T_TS > Time of
        true ->
	    %% dont sleep in case there is another read waiting
            %% timer:sleep((T_TS - Time) div 1000 +1 );
        %lager:info("Clock not ready"),
	    not_ready;
        false ->
        %lager:info("Clock ready"),
	    check_prepared(Key,TxId,State)
    end.

check_prepared(Key,TxId,State) ->
    SnapshotTime = TxId#tx_id.snapshot_time,
    ActiveTxs = 
	case ets:lookup(State#state.prepared_cache, Key) of
	    [] ->
		[];
	    [{Key,AList}] ->
		AList
	end,
    check_prepared_list(Key,SnapshotTime,ActiveTxs, ActiveTxs,State).

check_prepared_list(_Key,_SnapshotTime,[], _FullList, _State) ->
    ready;
check_prepared_list(Key,SnapshotTime,[{TxId, Time, Type, Op}|Rest], FullList,State) ->
    case Time =< SnapshotTime of
	    true ->
            case specula_utilities:should_specula(Time, SnapshotTime) of
                true ->
                    specula_utilities:make_prepared_specula(Key, {TxId, Time, Type, Op}, SnapshotTime, 
                        State#state.prepared_cache, State#state.snapshot_cache, State#state.specula_cache, 
                        State#state.specula_dep, FullList);
                false ->
                    ok 
            end,
	        not_ready;
	    false ->
	        check_prepared_list(Key,SnapshotTime,Rest, FullList, State)
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
return(Coordinator,Key, Type,TxId, SnapshotCache, _SpeculaCache) ->
    %lager:info("Returning for key ~w",[Key]),
    Reply = case ets:lookup(SnapshotCache, Key) of
                [] ->
                    {ok, {Type,Type:new()}};
                [{Key, ValueList}] ->
    %lager:info("Key is ~w, Transaciton is ~w, Valuelist ~w", [Key, TxId, ValueList]),
                    MyClock = TxId#tx_id.snapshot_time,
                    find_version(ValueList, MyClock, Type)
            end,
    case Coordinator of
        {sync, Sender} ->
            gen_server:reply(Sender, Reply);
        {async, Sender} ->
            %lager:info("Trying to reply for key ~w",[Key]),
            gen_fsm:send_event(Sender, Reply)
    end.

handle_info(_Info, StateData) ->
    {noreply,StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop,badmsg,StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop,badmsg,StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

%%%%%%%%%Intenal%%%%%%%%%%%%%%%%%%
find_version([], _SnapshotTime, Type) ->
    %{error, not_found};
    {ok, {Type,Type:new()}};
find_version([{TS, Value}|Rest], SnapshotTime, Type) ->
    case SnapshotTime >= TS of
        true ->
            {ok, {Type,Value}};
        false ->
            find_version(Rest, SnapshotTime, Type)
    end.
