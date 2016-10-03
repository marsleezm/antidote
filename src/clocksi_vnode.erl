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
-module(clocksi_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(NUM_VERSION, 44440).
-define(SPECULA_THRESHOLD, 0).

-export([start_vnode/1,
	    internal_read/3,
        debug_read/3,
	    relay_read/5,
        abort_others/5,
        get_size/1,
        %set_prepared/5,
        get_table/1,
        clean_data/2,
        async_send_msg/3,

        specula_commit/3,
        set_debug/2,
        do_reply/2,
        debug_prepare/4,
        prepare/3,
        prepare/4,
        commit/3,
        single_commit/2,
        append_value/5,
        append_values/4,
        abort/2,
        read_all/1,

        init/1,
        terminate/2,
        handle_command/3,
        is_empty/1,
        delete/1]).

-export([
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).


-ignore_xref([start_vnode/1]).

%%---------------------------------------------------------------------
%% @doc Data Type: state
%%      where:
%%          partition: the partition that the vnode is responsible for.
%%          prepared_txs: a list of prepared transactions.
%%          committed_tx: a list of committed transactions.
%%          downstream_set: a list of the downstream operations that the
%%              transactions generate.
%%          write_set: a list of the write sets that the transactions
%%              generate.
%%----------------------------------------------------------------------
-record(state, {partition :: non_neg_integer(),
                prepared_txs :: cache_id(),
                committed_txs :: cache_id(),
                if_certify :: boolean(),
                if_replicate :: boolean(),
                if_specula :: boolean(),
                inmemory_store :: cache_id(),
                dep_dict :: dict(),
                %l_abort_dict :: dict(),
                %r_abort_dict :: dict(),
                %Statistics
                %max_ts :: non_neg_integer(),
                debug = false :: boolean()
                %total_time :: non_neg_integer(),
                %prepare_count :: non_neg_integer(),
		        %relay_read :: {non_neg_integer(), non_neg_integer()},
                %num_specula_read :: non_neg_integer(),
                %num_aborted :: non_neg_integer(),
                %num_blocked :: non_neg_integer(),
                %num_cert_fail :: non_neg_integer(),
                %blocked_time :: non_neg_integer(),
                %num_committed :: non_neg_integer()
                }).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

set_debug(Node, Debug) ->
    riak_core_vnode_master:sync_command(Node,
                           {set_debug, Debug},
                           ?CLOCKSI_MASTER, infinity).

do_reply(Node, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                           {do_reply, TxId},
                           ?CLOCKSI_MASTER, infinity).

get_table(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                   {get_table},
                                   ?CLOCKSI_MASTER, infinity).

%% @doc Sends a read request to the Node that is responsible for the Key
internal_read(Node, Key, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                                   {internal_read, Key, TxId},
                                   ?CLOCKSI_MASTER, infinity).

debug_read(Node, Key, TxId) ->
    riak_core_vnode_master:sync_command(Node,
                                   {debug_read, Key, TxId},
                                   ?CLOCKSI_MASTER, infinity).

get_size(Node) ->
    riak_core_vnode_master:sync_command(Node,
                                   {get_size},
                                   ?CLOCKSI_MASTER, infinity).

read_all(Node) ->
    riak_core_vnode_master:command(Node,
                                   {read_all}, self(),
                                   ?CLOCKSI_MASTER).

clean_data(Node, From) ->
    riak_core_vnode_master:command(Node,
                                   {clean_data, From}, self(),
                                   ?CLOCKSI_MASTER).

relay_read(Node, Key, TxId, Reader, From) ->
    riak_core_vnode_master:command(Node,
                                   {relay_read, Key, TxId, Reader, From}, self(),
                                   ?CLOCKSI_MASTER).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(Updates, TxId, Type) ->
    ProposeTime = TxId#tx_id.snapshot_time+1,
    lists:foldl(fun({Node, WriteSet}, {Partitions, NumPartitions}) ->
        %lager:warning("Sending prepare to ~w", [Node]),
        riak_core_vnode_master:command(Node,
                           {prepare, TxId, WriteSet, Type, ProposeTime},
                           self(),
                           ?CLOCKSI_MASTER),
        {[Node|Partitions], NumPartitions+1}
    end, {[], 0}, Updates).

prepare(Updates, CollectedTS, TxId, Type) ->
    ProposeTS = max(TxId#tx_id.snapshot_time+1, CollectedTS),
    lists:foreach(fun({Node, WriteSet}) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId, WriteSet, Type, ProposeTS},
                               self(),
						       ?CLOCKSI_MASTER)
		end, Updates).

specula_commit(Partitions, TxId, SpeculaCommitTime) ->
    lists:foreach(fun(Partition) ->
			riak_core_vnode_master:command(Partition,
						       {specula_commit, TxId, SpeculaCommitTime},
                               self(), ?CLOCKSI_MASTER)
            end, Partitions).

debug_prepare(Updates, TxId, Type, Sender) ->
    ProposeTime = TxId#tx_id.snapshot_time+1,
    lists:foreach(fun({Node, WriteSet}) ->
			riak_core_vnode_master:command(Node,
						       {prepare, TxId, WriteSet, Type, ProposeTime},
                               Sender,
						       ?CLOCKSI_MASTER)
		end, Updates).

%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit(Node, WriteSet) ->
    riak_core_vnode_master:sync_command(Node,
                                   {single_commit, WriteSet},
                                   ?CLOCKSI_MASTER, infinity).

append_value(Node, Key, Value, CommitTime, ToReply) ->
    riak_core_vnode_master:command(Node,
                                   {append_value, Key, Value, CommitTime},
                                   ToReply,
                                   ?CLOCKSI_MASTER).

append_values(Node, KeyValues, CommitTime, _ToReply) ->
    riak_core_vnode_master:sync_command(Node,
                                   {append_values, KeyValues, CommitTime},
                                   ?CLOCKSI_MASTER, infinity).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(UpdatedParts, TxId, CommitTime) ->
    lists:foreach(fun(Node) ->
			riak_core_vnode_master:command(Node,
						       {commit, TxId, CommitTime},
						       {server, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, UpdatedParts).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
abort(UpdatedParts, TxId) ->
    lists:foreach(fun(Node) ->
			riak_core_vnode_master:command(Node,
						       {abort, TxId},
						       {server, undefined, self()},
						       ?CLOCKSI_MASTER)
		end, UpdatedParts).

%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    lager:info("Initing partition ~w", [Partition]),
    PreparedTxs = tx_utilities:open_public_table(prepared),
    CommittedTxs = tx_utilities:open_public_table(committed),
    InMemoryStore = tx_utilities:open_public_table(inmemory_store),
    DepDict = dict:new(),
    %DepDict1 = dict:store(success_wait, 0, DepDict),
    DepDict1 = dict:store(commit_diff, {0,0}, DepDict),
    %DepDict3 = dict:store(fucked_by_commit, {0,0}, DepDict2),
    %DepDict4 = dict:store(fucked_by_badprep, {0,0}, DepDict3),
    IfReplicate = antidote_config:get(do_repl), 
    IfSpecula = antidote_config:get(do_specula), 
    _ = case IfReplicate of
                    true ->
                        repl_fsm_sup:start_fsm(Partition);
                    false ->
                        ok
                end,
    %LD = dict:new(),
    %RD = dict:new(),
    {ok, #state{partition=Partition,
                committed_txs=CommittedTxs,
                prepared_txs=PreparedTxs,
		        %relay_read={0,0},
                %l_abort_dict=LD,
                %r_abort_dict=RD,
                if_replicate = IfReplicate,
                if_specula = IfSpecula,
                inmemory_store=InMemoryStore,
                dep_dict = DepDict1
                %total_time = 0, 
                %prepare_count = 0, 
                %num_aborted = 0,
                %num_blocked = 0,
                %blocked_time = 0,
                %num_specula_read = 0,
                %num_cert_fail = 0,
                %num_committed = 0
                }}.

handle_command({set_debug, Debug},_Sender,SD0=#state{partition=_Partition}) ->
    %lager:info("~w: Setting debug to be ~w", [Partition, Debug]),
    {reply, ok, SD0#state{debug=Debug}};

handle_command({get_table}, _Sender, SD0=#state{inmemory_store=InMemoryStore}) ->
    {reply, InMemoryStore, SD0};

handle_command({verify_table, Repl},_Sender,SD0=#state{inmemory_store=InMemoryStore}) ->
    R = helper:handle_verify_table(Repl, InMemoryStore),
    {reply, R, SD0};

handle_command({clean_data, Sender}, _Sender, SD0=#state{inmemory_store=InMemoryStore,
        prepared_txs=PreparedTxs, committed_txs=CommittedTxs, partition=Partition}) ->
    ets:delete_all_objects(InMemoryStore),
    ets:delete_all_objects(PreparedTxs),
    ets:delete_all_objects(CommittedTxs),
    DepDict = dict:new(),
    DepDict1 = dict:store(commit_diff, {0,0}, DepDict),
    Sender ! cleaned,
    {noreply, SD0#state{partition=Partition,
                dep_dict = DepDict1}};

handle_command({relay_read_stat},_Sender,SD0) -> %=#state{relay_read=RelayRead}) ->
    %R = helper:handle_relay_read_stat(RelayRead),
    {reply, 0, SD0};

handle_command({num_specula_read},_Sender,SD0) -> %=#state{num_specula_read=NumSpeculaRead}) ->
    {reply, 0, SD0};

handle_command({check_key_record, Key, Type},_Sender,SD0=#state{prepared_txs=PreparedTxs, committed_txs=CommittedTxs}) ->
    R = helper:handle_check_key_record(Key, Type, PreparedTxs, CommittedTxs),
    {reply, R, SD0};

handle_command({check_top_aborted, _},_Sender,SD0=#state{dep_dict=DepDict}) -> %=#state{l_abort_dict=LAbortDict, r_abort_dict=RAbortDict, dep_dict=DepDict}) ->
    R = helper:handle_check_top_aborted(DepDict),
    {reply, R, SD0};

handle_command({do_reply, TxId}, _Sender, SD0=#state{prepared_txs=PreparedTxs, if_replicate=IfReplicate}) ->
    [{{pending, TxId}, Result}] = ets:lookup(PreparedTxs, {pending, TxId}),
    ets:delete(PreparedTxs, {pending, TxId}),
    case IfReplicate of
        true ->
            %lager:warning("Start replicate ~w", [TxId]),
            {Sender, _RepMode, _WriteSet, PrepareTime} = Result,
            gen_server:cast(Sender, {prepared, TxId, PrepareTime}),
            {reply, ok, SD0};
        false ->
            %lager:warning("Start replying ~w", [TxId]),
            {From, Reply} = Result,
            gen_server:cast(From, Reply),
            {reply, ok, SD0}
    end;

handle_command({if_prepared, TxId, Keys}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    %lager:warning("Checking if prepared of ~w for ~w", [TxId, Keys]),
    Result = helper:handle_if_prepared(TxId, Keys, PreparedTxs), 
    {reply, Result, SD0};

handle_command({check_tables_ready},_Sender,SD0=#state{partition=Partition}) ->
    Result = helper:handle_check_tables_ready(Partition), 
    {reply, Result, SD0};

handle_command({print_stat},_Sender,SD0=#state{partition=_Partition %num_aborted=NumAborted, blocked_time=BlockedTime,
                    %num_committed=NumCommitted, num_cert_fail=NumCertFail, num_blocked=NumBlocked, total_time=A6, prepare_count=A7}) ->
                        }) ->
    %R = helper:handle_print_stat(Partition, NumAborted, BlockedTime,
    %                NumCommitted, NumCertFail, NumBlocked, A6, A7),
    {reply, ok, SD0};
    
handle_command({check_prepared_empty},_Sender,SD0=#state{prepared_txs=PreparedTxs}) ->
    R = helper:handle_command_check_prepared_empty(PreparedTxs),
    {reply, R, SD0};

handle_command({check_servers_ready},_Sender,SD0) ->
    {reply, true, SD0};

handle_command({debug_read, Key, TxId}, _Sender, SD0=#state{
            inmemory_store=InMemoryStore, partition=_Partition}) ->
    %MaxTS1 = max(TxId#tx_id.snapshot_time, MaxTS), 
    Result = read_value(Key, TxId, InMemoryStore),
    {reply, Result, SD0};

%% Internal read is not specula
handle_command({internal_read, Key, TxId}, Sender, SD0=#state{%num_blocked=NumBlocked, 
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, partition=_Partition}) ->
   %lager:info("Got read for ~w", [Key]),
    case local_cert_util:ready_or_block(TxId, Key, PreparedTxs, Sender) of
        not_ready->
            %lager:info("Not ready for ~w", [Key]),
            {noreply, SD0};
        ready ->
            Result = read_value(Key, TxId, InMemoryStore),
            %lager:info("Got value for ~w, ~w", [Key, Result]),
            {reply, Result, SD0}
    end;

handle_command({get_size}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, committed_txs=CommittedTxs}) ->
    TableSize = ets:info(InMemoryStore, memory) * erlang:system_info(wordsize),
    PrepareSize = ets:info(PreparedTxs, memory) * erlang:system_info(wordsize),
    CommittedSize = ets:info(CommittedTxs, memory) * erlang:system_info(wordsize),
    {reply, {PrepareSize, CommittedSize, TableSize, TableSize+PrepareSize+CommittedSize}, SD0};

handle_command({read_all}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
    Now = tx_utilities:now_microsec(),
    lists:foreach(fun({Key, _}) ->
            ets:insert(PreparedTxs, {Key, Now})
            end, ets:tab2list(InMemoryStore)),
    {noreply, SD0};

%% This read serves for all normal cases.
handle_command({relay_read, Key, TxId, Reader, SpeculaRead}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
    case SpeculaRead of
        false ->
            case local_cert_util:ready_or_block(TxId, Key, PreparedTxs, {TxId, ignore, {relay, Reader}}) of
                not_ready->
                    %lager:warning("Read for ~w of ~w blocked!", [Key, TxId]),
                    {noreply, SD0};
                ready ->
                    Result = read_value(Key, TxId, InMemoryStore),
                    %lager:warning("Read for ~w of ~w finished, Result is ~w! Replying to ~w", [Key, TxId, Result, Reader]),
                    gen_server:reply(Reader, Result), 
    		        %T2 = os:timestamp(),
                    {noreply, SD0}%i, relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}}
            end;
        true ->
            case local_cert_util:specula_read(TxId, Key, PreparedTxs, {TxId, ignore, {relay, Reader}}) of
                not_ready->
                    %lager:warning("Read blocked!"),
                    {noreply, SD0};
                {specula, Value} ->
                    gen_server:reply(Reader, {ok, Value}), 
    		        %T2 = os:timestamp(),
                     %lager:warning("Specula read finished: ~w, ~p", [TxId, Key]),
                    {noreply, SD0};
				        %relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}};
                ready ->
                    Result = read_value(Key, TxId, InMemoryStore),
                    %lager:warning("Read committed verions ~w", [Result]),
                    gen_server:reply(Reader, Result), 
    		        %T2 = os:timestamp(),
                    {noreply, SD0}
				        %relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}}
            end
    end;

%% RepMode:
%%  local: local certification, needs to send prepare to all slaves
%%  remote, AvoidNode: remote cert, do not need to send prepare to AvoidNode
handle_command({prepare, TxId, WriteSet, RepMode, ProposedTs}, RawSender,
               State = #state{partition=Partition,
                              if_specula=IfSpecula,
                              committed_txs=CommittedTxs,
                              prepared_txs=PreparedTxs,
                              dep_dict=DepDict,
                              debug=Debug
                              }) ->
    %lager:warning("Got prepare request for ~w at ~w", [TxId, Partition]),
    Sender = case RawSender of {debug, RS} -> RS; _ -> RawSender end,
    Result = local_cert_util:prepare_for_master_part(TxId, WriteSet, CommittedTxs, PreparedTxs, ProposedTs),
    case Result of
        {ok, PrepareTime} ->
            %UsedTime = tx_utilities:now_microsec() - PrepareTime,
            %lager:warning("~w: ~w certification check prepred with ~w, RepMode is ~w", [Partition, TxId, PrepareTime, RepMode]),
            case Debug of
                false ->
                    %case RepMode of local -> ok;
                    %                _ ->
                    PendingRecord = {Sender, RepMode, WriteSet, PrepareTime},
                    repl_fsm:repl_prepare(Partition, prepared, TxId, PendingRecord),
                    %end,
                    gen_server:cast(Sender, {prepared, TxId, PrepareTime, self()}),
                    {noreply, State};
                true ->
                    PendingRecord = {Sender, RepMode, WriteSet, PrepareTime},
                    repl_fsm:repl_prepare(Partition, prepared, TxId, PendingRecord),
                    ets:insert(PreparedTxs, {{pending, TxId}, PendingRecord}),
                    {noreply, State}
            end;
        {wait, PendPrepDep, PrepDep, PrepareTime} ->
            IfSpecula = true,
            NewDepDict 
                = case (PendPrepDep == 0) and (RepMode == local) of
                    true ->  
                        gen_server:cast(Sender, {pending_prepared, TxId, PrepareTime, self()}),
                        RepMsg = {Sender, pending_prepared, WriteSet, PrepareTime},
                        repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg),
                        dict:store(TxId, {0, PrepDep, PrepareTime, Sender, RepMode}, DepDict);
                    _ ->
                        dict:store(TxId, {PendPrepDep, PrepDep, PrepareTime, Sender, RepMode, WriteSet}, DepDict)
                  end, 
            {noreply, State#state{dep_dict=NewDepDict}};
        {error, write_conflict} ->
    %DepDict1 = dict:store(success_wait, 0, DepDict),
            %lager:warning("~w: ~w cerfify abort", [Partition, TxId]),
            case Debug of
                false ->
                    case RepMode of 
                        local ->
                            gen_server:cast(Sender, {aborted, TxId, {Partition, node()}}),
                            {noreply, State};
                        _ ->
                            gen_server:cast(Sender, {aborted, TxId, {Partition, node()}}),
                            {noreply, State}
                    end;
                true ->
                    ets:insert(PreparedTxs, {{pending, TxId}, {Sender, {aborted, TxId, RepMode}}}),
                    {noreply, State}
            end 
    end;

handle_command({specula_commit, TxId, SpeculaCommitTime}, _Sender, State=#state{prepared_txs=PreparedTxs,
        inmemory_store=InMemoryStore, dep_dict=DepDict, partition=Partition}) ->
    %lager:warning("Got specula commit for ~w", [TxId]),
    case ets:lookup(PreparedTxs, TxId) of
        [{TxId, Keys}] ->
            %repl_fsm:repl_prepare(Partition, prepared, TxId, RepMsg),
            DepDict1 = local_cert_util:specula_commit(Keys, TxId, SpeculaCommitTime, InMemoryStore, PreparedTxs, DepDict, Partition, master),
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
            lager:error("Prepared record of ~w has disappeared!", [TxId]),
            {noreply, State}
    end;

handle_command({append_value, Key, Value, CommitTime}, Sender,
               State = #state{committed_txs=CommittedTxs,
                              inmemory_store=InMemoryStore
                              }) ->
    ets:insert(CommittedTxs, {Key, CommitTime}),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}]});
        [{Key, ValueList}] ->
            {RemainList, _} = lists:split(min(?NUM_VERSION,length(ValueList)), ValueList),
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}|RemainList]})
    end,
    gen_server:reply(Sender, {ok, {committed, CommitTime}}),
    {noreply, State};

handle_command({append_values, KeyValues, CommitTime}, _Sender,
               State = #state{committed_txs=CommittedTxs,
                              inmemory_store=InMemoryStore
                              }) ->
     %lager:info("Got msg from ~p", [ToReply]),
    lists:foreach(fun({Key, Value}) ->
            ets:insert(CommittedTxs, {Key, CommitTime}),
            true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}]})
            end, KeyValues),
     %lager:info("Sending msg back to ~p", [ToReply]),
    %gen_fsm:reply(ToReply, {ok, {committed, CommitTime}}),
    {reply, {ok, committed}, State};

handle_command({commit, TxId, TxCommitTime}, _Sender,
               #state{partition=Partition,
                      committed_txs=CommittedTxs,
                      %if_replicate=IfReplicate,
                      prepared_txs=PreparedTxs,
                      inmemory_store=InMemoryStore,
                      dep_dict = DepDict,
                      %num_committed=NumCommitted,
                      if_specula=IfSpecula
                      } = State) ->
    %lager:warning("~w: Got commit req for ~w with ~w", [Partition, TxId, TxCommitTime]),
    Result = commit(TxId, TxCommitTime, CommittedTxs, PreparedTxs, InMemoryStore, DepDict, Partition, IfSpecula),
    case Result of
        {ok, committed, DepDict1} ->
            {noreply, State#state{dep_dict=DepDict1}};
        {error, no_updates} ->
            {reply, no_tx_record, State}
    end;

handle_command({abort, TxId}, _Sender,
               State = #state{partition=Partition, prepared_txs=PreparedTxs, inmemory_store=InMemoryStore,
                dep_dict=DepDict, if_specula=IfSpecula}) ->
    %lager:warning("~w: Aborting ~w", [Partition, TxId]),
    case ets:lookup(PreparedTxs, TxId) of
        [{TxId, Keys}] ->
            case IfSpecula of
                true -> specula_utilities:deal_abort_deps(TxId);
                false -> ok
            end,
            %lager:warning("Found key set"),
            true = ets:delete(PreparedTxs, TxId),
            DepDict1 = local_cert_util:clean_abort_prepared(PreparedTxs, Keys, TxId, InMemoryStore, DepDict, Partition, master),
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
            %lager:error("No key set at all for ~w", [TxId]),
            {noreply, State}
    end;

handle_command({start_read_servers}, _Sender, State) ->
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(StatName, Val) ->
    term_to_binary({StatName,Val}).

is_empty(State) ->
    {true,State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{prepared_txs=PreparedTxs, committed_txs=CommittedTxs, 
                            inmemory_store=InMemoryStore} = _State) ->
    ets:delete(PreparedTxs),
    ets:delete(CommittedTxs),
    ets:delete(InMemoryStore),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================
async_send_msg(Delay, Msg, To) ->
    timer:sleep(Delay),
    riak_core_vnode_master:command(To, Msg, To, ?CLOCKSI_MASTER).

%set_prepared(_PreparedTxs,[],_TxId,_Time, KeySet) ->
%    KeySet;
%set_prepared(PreparedTxs,[{Key, Value} | Rest],TxId,Time, KeySet) ->
%    true = ets:insert(PreparedTxs, {Key, {TxId, Time, Value, [], []}}),
%    set_prepared(PreparedTxs,Rest,TxId,Time, [Key|KeySet]).

commit(TxId, TxCommitTime, CommittedTxs, PreparedTxs, InMemoryStore, DepDict, Partition, IfSpecula)->
    %lager:warning("Before commit ~w", [TxId]),
    case ets:lookup(PreparedTxs, TxId) of
        [{TxId, Keys}] ->
            case IfSpecula of
                true -> specula_utilities:deal_commit_deps(TxId, TxCommitTime);
                false -> ok
            end,
            DepDict1 = local_cert_util:update_store(Keys, TxId, TxCommitTime, InMemoryStore, CommittedTxs, 
                PreparedTxs, DepDict, Partition, master),
            true = ets:delete(PreparedTxs, TxId),
            {ok, committed, DepDict1};
        [] ->
            lager:error("Prepared record of ~w has disappeared!", [TxId]),
            error
    end.

%% @doc clean_and_notify:
%%      This function is used for cleanning the state a transaction
%%      stores in the vnode while it is being procesed. Once a
%%      transaction commits or aborts, it is necessary to clean the 
%%      prepared record of a transaction T. There are three possibility
%%      when trying to clean a record:
%%      1. The record is prepared by T (with T's TxId).
%%          If T is being committed, this is the normal. If T is being 
%%          aborted, it means T successfully prepared here, but got 
%%          aborted somewhere else.
%%          In both cases, we should remove the record.
%%      2. The record is empty.
%%          This can only happen when T is being aborted. What can only
%%          only happen is as follows: when T tried to prepare, someone
%%          else has already prepared, which caused T to abort. Then 
%%          before the partition receives the abort message of T, the
%%          prepared transaction gets processed and the prepared record
%%          is removed.
%%          In this case, we don't need to do anything.
%%      3. The record is prepared by another transaction M.
%%          This can only happen when T is being aborted. We can not
%%          remove M's prepare record, so we should not do anything
%%          either. 
%%



abort_others(_, [], DepDict, _MyNode, Readers) ->
    {DepDict, [], Readers};
abort_others(PPTime, [{_Type, TxId, _PTime, _Value, PendingReaders}|Rest]=NonAborted, DepDict, MyNode, Readers) ->
    case PPTime > TxId#tx_id.snapshot_time of
        true ->
            case dict:find(TxId, DepDict) of
                {ok, {_, _, Sender, _}} ->
                    NewDepDict = dict:erase(TxId, DepDict),
                    gen_server:cast(Sender, {aborted, TxId, MyNode}),
                   %lager:warning("Aborting ~w, because PPTime ~w is larger", [TxId, PPTime]),
                    abort([MyNode], TxId),
                    abort_others(PPTime, Rest, NewDepDict, MyNode, PendingReaders++Readers);
                error ->
                   %lager:warning("~w aborted already", [TxId]),
                    abort_others(PPTime, Rest, DepDict, MyNode, PendingReaders ++ Readers)
            end;
        false ->
            {DepDict, NonAborted, Readers}
    end.
        

read_value(Key, TxId, InMemoryStore) ->
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            {ok, []};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            find_version(ValueList, MyClock)
    end.

find_version([],  _SnapshotTime) ->
    {ok, []};
find_version([{TS, Value}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {ok, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.
