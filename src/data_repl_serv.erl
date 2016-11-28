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
%%
%% TODO: Two problems for read
%%      1. A replica is only allowed to serve a request if it's timestamp
%%       is higher than the snapshot time. The replica should update its 
%%       timestamp after it receives a commit from master.
%%      2. A replica needs to handles commit/abort sequentially. Basically
%%       if the timestamp of abort/commit it received is higher than
%%       the pending ones, it has to wait (can not update it's timestamp).
%%

-module(data_repl_serv).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(NUM_VERSIONS, 40).
-define(READ_TIMEOUT, 15000).
%% API
-export([start_link/2]).

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
-export([relay_read/4,
        get_table/1,
        get_size/1,
        read_all/1,
	    check_key/2,
        get_ts/4,
        clean_data/2,
	    check_table/1,
        verify_table/2,
        debug_read/3,
        prepare_specula/5,
        local_certify/4,
        pre_commit/4,
        %commit_specula/4,
        %abort_specula/3,
        if_prepared/3,
        if_bulk_prepared/3,
        num_specula_read/1,
        single_read/2,
    
        read/4]).

%% Spawn

-record(state, {
        successors :: [atom()],
        inmemory_store :: cache_id(),
        prepared_txs :: cache_id(),
        committed_txs :: cache_id(),
        dep_dict :: dict(),
        delay :: non_neg_integer(),
        table_size = 0,
        %init_ts_dict=false :: boolean(),
        ts :: non_neg_integer(),
        num_specula_read=0 :: non_neg_integer(),
        num_read=0 :: non_neg_integer(),
        set_size :: non_neg_integer(),
        current_dict :: dict(),
        backup_dict :: dict(),
        %current_dict :: cache_id(),
        %backup_dict :: cache_id(),
        %ts_dict :: dict(),
        specula_read :: boolean(),
        name :: atom(),
		self :: atom()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Name, Parts) ->
    gen_server:start_link({global, Name},
             ?MODULE, [Name, Parts], []).

read(Name, Key, TxId, Part) ->
    gen_server:call({global, Name}, {read, Key, TxId, Part}, ?READ_TIMEOUT).

get_table(Name) ->
    gen_server:call({global, Name}, {get_table}, ?READ_TIMEOUT).

get_size(Name) ->
    gen_server:call({global, Name}, {get_size}, ?READ_TIMEOUT).

get_ts(Name, TxId, Partition, WriteSet) ->
    gen_server:call({global, Name}, {get_ts, TxId, Partition, WriteSet}, ?READ_TIMEOUT).

single_read(Name, Key) ->
    TxId = tx_utilities:create_tx_id(0),
    gen_server:call({global, Name}, {read, Key, TxId}, ?READ_TIMEOUT).

verify_table(Name, List) ->
    gen_server:call({global, Name}, {verify_table, List}, infinity).

check_table(Name) ->
    gen_server:call({global, Name}, {check_table}).

check_key(Name, Key) ->
    gen_server:call({global, Name}, {check_key, Key}).

num_specula_read(Node) ->
    gen_server:call({global, Node}, {num_specula_read}).

debug_read(Name, Key, TxId) ->
    gen_server:call({global, Name}, {debug_read, Key, TxId}).

if_prepared(Name, TxId, Keys) ->
    gen_server:call({global, Name}, {if_prepared, TxId, Keys}).

if_bulk_prepared(Name, TxId, Partition) ->
    gen_server:call({global, Name}, {if_bulk_prepared, TxId, Partition}).

prepare_specula(Name, TxId, Partition, WriteSet, PrepareTime) ->
    gen_server:cast({global, Name}, {prepare_specula, TxId, Partition, WriteSet, PrepareTime}).

local_certify(Name, TxId, Partition, WriteSet) ->
    gen_server:cast({global, Name}, {local_certify, TxId, Partition, WriteSet, self()}).

read_all(Name) ->
    gen_server:cast({global, Name}, {get_size}).

pre_commit(Name, TxId, Partition, SpeculaCommitTs) ->
    gen_server:cast({global, Name}, {pre_commit, TxId, Partition, SpeculaCommitTs}).

relay_read(Name, Key, TxId, Reader) ->
    gen_server:cast({global, Name}, {relay_read, Key, TxId, Reader}).

clean_data(Name, Sender) ->
    gen_server:cast({global, Name}, {clean_data, Sender}).


%commit_specula(Name, TxId, Partition, CommitTime) ->
%    gen_server:cast({global, Name}, {commit_specula, TxId, Partition, CommitTime}).

%abort_specula(Name, TxId, Partition) ->
%    gen_server:cast({global, Name}, {abort_specula, TxId, Partition}).

%%%===================================================================
%%% Internal
%%%===================================================================


init([Name, _Parts]) ->
    InMemoryStore = tx_utilities:open_public_table(repl_log),
    PreparedTxs = tx_utilities:open_private_table(prepared_txs),
    CommittedTxs = tx_utilities:open_private_table(committed_txs),
    %NumPartitions = length(hash_fun:get_partitions()),
    [{_, Replicas}] = ets:lookup(meta_info, node()),
    TotalReplFactor = length(Replicas)+1,
    Concurrent = antidote_config:get(concurrent),
    SpeculaLength = antidote_config:get(specula_length),
    SetSize = min(max(TotalReplFactor*10*Concurrent*max(SpeculaLength,6), 5000), 40000),
    lager:info("Data repl inited with name ~w, repl factor is ~w, set size is ~w", [Name, TotalReplFactor, SetSize]),
    SpeculaRead = antidote_config:get(specula_read),
    %TsDict = lists:foldl(fun(Part, Acc) ->
    %            dict:store(Part, 0, Acc) end, dict:new(), Parts),
    %lager:info("Parts are ~w, TsDict is ~w", [Parts, dict:to_list(TsDict)]),
    %lager:info("Concurrent is ~w, num partitions are ~w", [Concurrent, NumPartitions]),
    {ok, #state{name=Name, set_size=SetSize, specula_read=SpeculaRead,
                prepared_txs = PreparedTxs, current_dict = dict:new(), committed_txs=CommittedTxs, dep_dict=dict:new(), 
                backup_dict = dict:new(), inmemory_store = InMemoryStore}}.

handle_call({get_table}, _Sender, SD0=#state{inmemory_store=InMemoryStore}) ->
    {reply, InMemoryStore, SD0};

handle_call({get_pid}, _Sender, SD0) ->
        {reply, self(), SD0};

handle_call({get_size}, _Sender, SD0=#state{
          prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, committed_txs=CommittedTxs}) ->
  TableSize = ets:info(InMemoryStore, memory) * erlang:system_info(wordsize),
  PrepareSize = ets:info(PreparedTxs, memory) * erlang:system_info(wordsize),
  CommittedSize = ets:info(CommittedTxs, memory) * erlang:system_info(wordsize),
  {reply, {PrepareSize, CommittedSize, TableSize, TableSize+PrepareSize+CommittedSize}, SD0};

handle_call({retrieve_log, LogName},  _Sender,
	    SD0=#state{inmemory_store=InMemoryStore}) ->
    case ets:lookup(InMemoryStore, LogName) of
        [{LogName, Log}] ->
            {reply, Log, SD0};
        [] ->
            {reply, [], SD0}
    end;


handle_call({num_specula_read}, _Sender, SD0=#state{num_specula_read=NumSpeculaRead, num_read=NumRead}) ->
    {reply, {NumSpeculaRead, NumRead}, SD0};

handle_call({check_table}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    lager:info("Log info: ~w", [ets:tab2list(PreparedTxs)]),
    {reply, ok, SD0};

handle_call({check_key, Key}, _Sender, SD0=#state{inmemory_store=InMemoryStore}) ->
    Result = ets:lookup(InMemoryStore, Key),
    {reply, Result, SD0};

handle_call({debug_read, Key, TxId}, _Sender, 
	    SD0=#state{inmemory_store=InMemoryStore}) ->
    lager:info("Got debug read for ~w from ~p", [Key, TxId]),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            lager:info("Debug reading ~w, there is nothing", [Key]),
            {reply, {ok, []}, SD0};
        [{Key, ValueList}] ->
            lager:info("Debug reading ~w, Value list is ~w", [Key, ValueList]),
            MyClock = TxId#tx_id.snapshot_time,
            case find_nonspec_version(ValueList, MyClock) of
                Value ->
                    lager:info("Found value is ~w", [Value]),
                    {reply, {ok, Value}, SD0}
            end
    end;

%% The real key is be read as {Part, Key} 
handle_call({read, Key, TxId, _Node}, Sender, 
	    SD0=#state{inmemory_store=InMemoryStore, prepared_txs=PreparedTxs,
                specula_read=SpeculaRead}) ->
   %lager:warning("Data repl reading ~w ~w", [TxId, Key]),
    case SpeculaRead of
        false ->
           %lager:warning("Specula rea on data repl and false!!??"),
            case local_cert_util:ready_or_block(TxId, Key, PreparedTxs, {TxId, ignore, {relay, Sender}}) of
                not_ready-> {noreply, SD0};
                ready ->
                    %lager:warning("Read finished!"),
                    Result = read_value(Key, TxId, InMemoryStore),
                    %T2 = os:timestamp(),
                    {reply, Result, SD0}%i, relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}}
            end;
        true ->
            %lager:warning("Specula read!!"),
            case local_cert_util:specula_read(TxId, Key, PreparedTxs, {TxId, ignore, {relay, Sender}}) of
                not_ready->
                    %lager:warning("Read blocked!"),
                    {noreply, SD0};
                {specula, Value} ->
                    {reply, {ok, Value}, SD0};
                        %relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}};
                ready ->
                    %lager:warning("Read finished!"),
                    Result = read_value(Key, TxId, InMemoryStore),
                    %T2 = os:timestamp(),
                    {reply, Result, SD0}
                        %relay_read={NumRR+1, AccRR+get_time_diff(T1, T2)}}}
            end
    end;

handle_call({update_ts, _Partitions}, _Sender, SD0) ->
    {reply, ok, SD0};
    %case InitTs of true ->  {reply, ok, SD0};
    %               false ->  
    %                        Parts = find_parts_for_name(Partitions),
    %                        TsDict1 = lists:foldl(fun(Part, D) ->
     %                           dict:store(Part, 0, D)
     %                           end, TsDict, Parts),
    %                        {reply, ok, SD0#state{ts_dict=TsDict1, init_ts_dict=false}}
    %end;

handle_call({get_ts, TxId, Partition, WriteSet}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
        [] ->
            MaxTs = lists:foldl(fun({Key, _Value}, ToPrepTS) ->
                        case ets:lookup(PreparedTxs, Key) of
                            [] ->
                                ToPrepTS;
                            [{Key, [{_PrepTxId, _PrepTS, LastReaderTS, _PrepValue, _Reader}|_RemainList]}] ->
                                max(ToPrepTS, LastReaderTS+1);
                            [{Key, LastReaderTS}] ->
                                %lager:info("LastReader ts is ~p", [LastReaderTS]),
                                max(ToPrepTS, LastReaderTS+1)
                        end end, 0, WriteSet),
            %% Leave a tag here so that a prepare message arrive in-between will not reply more (which causes prepare to go negative)
            ets:insert(PreparedTxs, {{TxId, Partition}, ignore}),
             %lager:warning("~w ~w got ts", [TxId, Partition]),
            {reply, MaxTs, SD0};
        _ ->
           %lager:warning("~w ~w prepared already", [TxId, Partition]),
            {reply, exist, SD0}
    end;

handle_call({if_prepared, TxId, Keys}, _Sender, SD0=#state{prepared_txs=PreparedTxs}) ->
    Result = helper:handle_if_prepared(TxId, Keys, PreparedTxs),
    {reply, Result, SD0};

handle_call({if_bulk_prepared, TxId, Partition}, _Sender, SD0=#state{
            prepared_txs=PreparedTxs}) ->
    lager:info("checking if bulk_prepared for ~w ~w", [TxId, Partition]),
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
        [{{TxId, Partition}, _}] ->
            lager:info("It's inserted"),
            {reply, true, SD0};
        Record ->
            lager:info("~w: something else", [Record]),
            {reply, false, SD0}
    end;

handle_call({verify_table, List}, _Sender, SD0=#state{name=Name, inmemory_store=InMemoryStore}) ->
   lager:info("Start verifying on ~w", [Name]),
   lists:foreach(fun({K, V}=Elem) -> 
                    case ets:lookup(InMemoryStore, K) of [Elem] -> ok;
                        Other -> 
                            lager:error("Doesn't match! Origin is [{~p, ~p}], Rep is ~p", [K, V, Other])
                    end end, List),
   {reply, ok, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({relay_read, Key, TxId, Reader}, 
	    SD0=#state{inmemory_store=InMemoryStore}) ->
      %lager:warning("~w, ~p data repl read", [TxId, Key]),
    case ets:lookup(InMemoryStore, Key) of
        [] ->
              %lager:warning("Nothing for ~p!", [Key]),
            gen_server:reply(Reader, {ok, []}),
            {noreply, SD0};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            Value = find_version(ValueList, MyClock),
              %lager:warning("Got value for ~p", [ValueList, Key]),
            gen_server:reply(Reader, Value),
            {noreply, SD0}
    end;

handle_cast({read_all}, SD0=#state{
            prepared_txs=PreparedTxs, inmemory_store=InMemoryStore}) ->
    Now = tx_utilities:now_microsec(),
    lists:foreach(fun({Key, _}) ->
            ets:insert(PreparedTxs, {Key, Now})
            end, ets:tab2list(InMemoryStore)),
    {noreply, SD0};

handle_cast({prepare_specula, TxId, Part, WriteSet, ToPrepTS}, 
	    SD0=#state{prepared_txs=PreparedTxs}) ->
    KeySet = lists:foldl(fun({Key, Value}, KS) ->
                      case ets:lookup(PreparedTxs, Key) of
                          [] ->
                              true = ets:insert(PreparedTxs, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}]}),
                              [Key|KS];
                          [{Key, [{PrepTxId, PrepTS, _LastReaderTS, PrepValue, Reader}|RemainList]}] ->
                              true = ets:insert(PreparedTxs, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}|[{PrepTxId, PrepTS, PrepValue, Reader}|RemainList]]}),
                              [Key|KS];
                          [{Key, _}] ->
                              true = ets:insert(PreparedTxs, {Key, [{TxId, ToPrepTS, ToPrepTS, Value, []}]}),
                              [Key|KS]
                      end end, [], WriteSet),
    ets:insert(PreparedTxs, {{TxId, Part}, KeySet}),
   %lager:warning("Specula prepared for [~w, ~w]", [TxId, Part]),
    {noreply, SD0};

%% Need to certify here
handle_cast({local_certify, TxId, Partition, WriteSet, Sender}, 
	    SD0=#state{prepared_txs=PreparedTxs, committed_txs=CommittedTxs, dep_dict=DepDict}) ->
   %lager:warning("local certify for [~w, ~w]", [TxId, Partition]),
    Result = local_cert_util:prepare_for_other_part(TxId, Partition, WriteSet, CommittedTxs, PreparedTxs, TxId#tx_id.snapshot_time+1, slave),
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
        inmemory_store=InMemoryStore, dep_dict=DepDict}) ->
   %lager:warning("Specula commit for ~w ", [Partition]),
    case ets:lookup(PreparedTxs, {TxId, Partition}) of
        [{{TxId, Partition}, Keys}] ->
            DepDict1 = local_cert_util:pre_commit(Keys, TxId, SpeculaCommitTime, InMemoryStore, PreparedTxs, DepDict, Partition, slave),
            {noreply, State#state{dep_dict=DepDict1}};
        [] ->
            lager:error("Prepared record of ~w, ~w has disappeared!", [TxId, Partition]),
            error
    end;

handle_cast({clean_data, Sender}, SD0=#state{inmemory_store=InMemoryStore, prepared_txs=PreparedTxs,
            committed_txs=CommittedTxs}) ->
    %lager:info("Got request!"),
    ets:delete_all_objects(PreparedTxs),
    ets:delete_all_objects(InMemoryStore),
    ets:delete_all_objects(CommittedTxs),
    %ets:delete(CurrentDict),
    %ets:delete(BackupDict),
    lager:info("Data repl replying!"),
    Sender ! cleaned,
    {noreply, SD0#state{prepared_txs = PreparedTxs, current_dict = dict:new(), %tx_utilities:open_private_table(current), 
                backup_dict = dict:new(), %tx_utilities:open_private_table(backup), 
                table_size=0, 
                num_specula_read=0, num_read=0, inmemory_store = InMemoryStore, committed_txs=CommittedTxs}};

%% Where shall I put the speculative version?
%% In ets, faster for read.
%handle_cast({abort_specula, TxId, Partition}, 
%	    SD0=#state{inmemory_store=InMemoryStore, prepared_txs=PreparedTxs, ts_dict=TsDict}) ->
%    lager:info("Abort specula for ~w, ~w", [TxId, Partition]),
    %TsDict1 = lists:foldl(fun(Partition, D) ->
%                [{{TxId, Partition}, KeySet}] = ets:lookup(PreparedTxs, {TxId, Partition}),
%                ets:delete(PreparedTxs, {TxId, Partition}),
%                MaxTs = clean_abort_prepared(PreparedTxs, KeySet, TxId, InMemoryStore, 0),
%                TsDict1= dict:update(Partition, fun(OldTs) -> max(MaxTs, OldTs) end, TsDict),
     %     end, TsDict, Partitions),
%    specula_utilities:deal_abort_deps(TxId),
%    {noreply, SD0#state{ts_dict=TsDict1}};
    
%handle_cast({commit_specula, TxId, Partition, CommitTime}, 
%	    SD0=#state{inmemory_store=InMemoryStore, prepared_txs=PreparedTxs, ts_dict=TsDict}) ->
%    %lager:warning("Committing specula for ~w ~w", [TxId, Partition]),
    %TsDict1 = lists:foldl(fun(Partition, D) ->
%              [{{TxId, Partition}, KeySet}] = ets:lookup(PreparedTxs, {TxId, Partition}),
%              ets:delete(PreparedTxs, {TxId, Partition}),
%              MaxTs = update_store(KeySet, TxId, CommitTime, InMemoryStore, PreparedTxs, 0),
%    TsDict1 = dict:update(Partition, fun(OldTs) -> max(MaxTs, OldTs) end, TsDict),
    %      end, TsDict, Partitions),
%    specula_utilities:deal_commit_deps(TxId, CommitTime),
%    {noreply, SD0#state{ts_dict=TsDict1}};

handle_cast({repl_prepare, Type, TxId, Part, WriteSet, TimeStamp, Sender}, 
	    SD0=#state{prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, current_dict=CurrentDict, backup_dict=BackupDict}) ->
    case Type of
        prepared ->
            case dict:find(TxId, CurrentDict) of
                {ok, finished} ->
                    {noreply, SD0};
                error ->
            %case ets:lookup(CurrentDict, TxId) of
            %    [{TxId, finished}] ->
            %        {noreply, SD0};
            %    [] ->
                    case dict:find(TxId, BackupDict) of
                        {ok, finished} ->
                            {noreply, SD0};
                        error ->
                    %case ets:lookup(BackupDict, TxId) of
                    %    [{TxId, finished}] ->
                    %        {noreply, SD0};
                    %    [] ->
                            %lager:warning("Got repl prepare for ~w, ~w", [TxId, Part]),
                            local_cert_util:insert_prepare(PreparedTxs, TxId, Part, WriteSet, TimeStamp, Sender),
                            {noreply, SD0}
                    end
            end;
        single_commit ->
            AppendFun = fun({Key, Value}) ->
                case ets:lookup(InMemoryStore, Key) of
                    [] ->
                         %lager:warning("Data repl inserting ~p, ~p of ~w to table", [Key, Value, TimeStamp]),
                        true = ets:insert(InMemoryStore, {Key, [{TimeStamp, Value}]});
                    [{Key, ValueList}] ->
                        {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
                        true = ets:insert(InMemoryStore, {Key, [{TimeStamp, Value}|RemainList]})
                end end,
            lists:foreach(AppendFun, WriteSet),
            gen_server:cast({global, Sender}, {ack, Part, TxId}), 
            {noreply, SD0}
    end;

handle_cast({repl_commit, TxId, CommitTime, Partitions}, 
	    SD0=#state{inmemory_store=InMemoryStore, prepared_txs=PreparedTxs, specula_read=SpeculaRead, committed_txs=CommittedTxs,
        dep_dict=DepDict}) ->
  %lager:warning("Repl commit for ~w, ~w", [TxId, Partitions]),
   DepDict1 = lists:foldl(fun(Partition, D) ->
                    [{{TxId, Partition}, KeySet}] = ets:lookup(PreparedTxs, {TxId, Partition}), 
                    ets:delete(PreparedTxs, {TxId, Partition}),
                    local_cert_util:update_store(KeySet, TxId, CommitTime, InMemoryStore, CommittedTxs, PreparedTxs, 
                        D, Partition, slave)
                    %end
        end, DepDict, Partitions),
    case SpeculaRead of
        true -> specula_utilities:deal_commit_deps(TxId, CommitTime); 
        _ -> ok
    end,
    {noreply, SD0#state{dep_dict=DepDict1}};
    %case dict:size(CurrentD1) > SetSize of
    %      true ->
    %        {noreply, SD0#state{ts_dict=TsDict1, current_dict=dict:new(), backup_dict=CurrentD1}};
    %      false ->
    %        {noreply, SD0#state{ts_dict=TsDict1, current_dict=CurrentD1}}
    %end;
handle_cast({repl_abort, TxId, Partitions, local}, 
	    SD0=#state{prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, specula_read=SpeculaRead, dep_dict=DepDict}) ->
   %lager:warning("repl abort for ~w ~w", [TxId, Partitions]),
    DepDict1 = lists:foldl(fun(Partition, DepD) ->
               case ets:lookup(PreparedTxs, {TxId, Partition}) of
                    [{{TxId, Partition}, KeySet}] ->
                        %lager:warning("Found for ~w, ~w, KeySet is ~w", [TxId, Partition, KeySet]),
                        ets:delete(PreparedTxs, {TxId, Partition}),
                        DepD1= local_cert_util:clean_abort_prepared(PreparedTxs, KeySet, TxId, InMemoryStore, DepD, Partition, slave),
                        DepD1;
                    [] ->
                        DepD
                end
        end, DepDict, Partitions),
    %%% This needs to be stored because a prepare request may come twice: once from specula_prep and once 
    %%  from tx coord
    case SpeculaRead of
        true -> specula_utilities:deal_abort_deps(TxId);
        _ -> ok
    end,
    {noreply, SD0#state{dep_dict=DepDict1}};

handle_cast({repl_abort, TxId, Partitions}, 
	    SD0=#state{prepared_txs=PreparedTxs, inmemory_store=InMemoryStore, specula_read=SpeculaRead, current_dict=CurrentDict, dep_dict=DepDict, set_size=SetSize, table_size=TableSize}) ->
   %lager:warning("repl abort for ~w ~w", [TxId, Partitions]),
    {IfMissed, DepDict1} = lists:foldl(fun(Partition, {IfMiss, DepD}) ->
               case ets:lookup(PreparedTxs, {TxId, Partition}) of
                    [{{TxId, Partition}, KeySet}] ->
                        %lager:warning("Found for ~w, ~w, KeySet is ~w", [TxId, Partition, KeySet]),
                        ets:delete(PreparedTxs, {TxId, Partition}),
                        DepD1= local_cert_util:clean_abort_prepared(PreparedTxs, KeySet, TxId, InMemoryStore, DepD, Partition, slave),
                        {IfMiss, DepD1};
                    [] ->
                       %lager:warning("Repl abort arrived early! ~w", [TxId]),
                        %{dict:store(TxId, finished, D), DepD}
                        {true, DepD}
                end
        end, {false, DepDict}, Partitions),
    %%% This needs to be stored because a prepare request may come twice: once from specula_prep and once 
    %%  from tx coord
    case SpeculaRead of
        true -> specula_utilities:deal_abort_deps(TxId);
        _ -> ok
    end,
    case IfMissed of
        true ->
            %ets:insert(CurrentDict, {TxId, finished}),
            CurrentDict1 = dict:store(TxId, finished, CurrentDict),
            case TableSize > SetSize of
                true ->
                    %lager:warning("Current set is too large!"),
                    %ets:delete(BackupDict),
                    {noreply, SD0#state{current_dict=dict:new(), backup_dict=CurrentDict1, table_size=0, dep_dict=DepDict1}};
                false ->
                    {noreply, SD0#state{current_dict=CurrentDict1, dep_dict=DepDict1, table_size=TableSize+1}}
            end;
        false ->
            {noreply, SD0#state{dep_dict=DepDict1}}
    end;

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

read_value(Key, TxId, InMemoryStore) ->
    case ets:lookup(InMemoryStore, Key) of
        [] ->
            {ok, []};
        [{Key, ValueList}] ->
            MyClock = TxId#tx_id.snapshot_time,
            find_version(ValueList, MyClock)
    end.

find_nonspec_version([],  _SnapshotTime) ->
    [];
find_nonspec_version([{TS, Value}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            Value;
        false ->
            find_version(Rest, SnapshotTime)
    end;
find_nonspec_version([{_, _, _}|Rest], SnapshotTime) ->
    find_version(Rest, SnapshotTime).


find_version([],  _SnapshotTime) ->
    {ok, []};
find_version([{TS, Value}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {ok, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end;
find_version([{TS, Value, TxId}|Rest], SnapshotTime) ->
    case SnapshotTime >= TS of
        true ->
            {specula, TxId, Value};
        false ->
            find_version(Rest, SnapshotTime)
    end.


%append_by_parts(_, _, _, _, []) ->
%    ok;
%append_by_parts(PreparedTxs, InMemoryStore, TxId, CommitTime, [Part|Rest]) ->
%    case ets:lookup(PreparedTxs, {TxId, Part}) of
%        [{{TxId, Part}, {WriteSet, _}}] ->
%             %lager:warning("For ~w ~w found writeset", [TxId, Part, WriteSet]),
%            AppendFun = fun({Key, Value}) ->
%                             %lager:warning("Adding ~p, ~p wth ~w of ~w into log", [Key, Value, CommitTime, TxId]),
%                            case ets:lookup(InMemoryStore, Key) of
%                                [] ->
%                                    true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}]});
%                                [{Key, ValueList}] ->
%                                    {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
%                                    true = ets:insert(InMemoryStore, {Key, [{CommitTime, Value}|RemainList]})
%                            end end,
%            lists:foreach(AppendFun, WriteSet),
%            ets:delete(PreparedTxs, {TxId, Part});
%        [] ->
%	     % %lager:warning("Commit ~w ~w arrived early! Committing with ~w", [TxId, Part, CommitTime]),
%	        ets:insert(PreparedTxs, {{TxId, Part}, CommitTime})
%    end,
%    append_by_parts(PreparedTxs, InMemoryStore, TxId, CommitTime, Rest). 

%abort_by_parts(_, _, [], Set) ->
%    Set;
%abort_by_parts(PreparedTxs, TxId, [Part|Rest], Set) ->
%    case ets:lookup(PreparedTxs, {TxId, Part}) %of
%	    [] ->
 %           Set1 = sets:add_element({TxId, Part}, Set),
 %           abort_by_parts(PreparedTxs, TxId, Rest, Set1); 
%	    _ ->
 %   	    ets:delete(PreparedTxs, {TxId, Part}),
 %           abort_by_parts(PreparedTxs, TxId, Rest, Set) 
 %   end.


%delete_version([{_, _, TxId}|Rest], TxId) -> 
%    Rest;
%delete_version([{C, V}|Rest], TxId) -> 
%    [{C, V}|delete_version(Rest, TxId)];
%delete_version([{TS, V, TId}|Rest], TxId) -> 
%    [{TS, V, TId}|delete_version(Rest, TxId)].

%replace_version([{_, Value, TxId}|Rest], TxId, CommitTime) -> 
%    [{CommitTime, Value}|Rest];
%replace_version([{C, V}|Rest], TxId, CommitTime) -> 
%    [{C, V}|replace_version(Rest, TxId, CommitTime)];
%replace_version([{TS, V, PrepTxId}|Rest], TxId, CommitTime) -> 
%    [{TS, V, PrepTxId}|replace_version(Rest, TxId, CommitTime)].

%%% A list that has timestamp in descending order
%insert_version([], MyTxId, MyPrepTime, MyValue) -> 
%    [{MyTxId, MyPrepTime, MyValue, []}];
%insert_version([{_TxId, Timestamp, _Value, _Reader}|_Rest]=VList, MyTxId, MyPrepTime, MyValue) when MyPrepTime >= Timestamp -> 
%    [{MyTxId, MyPrepTime, MyValue, []}| VList];
%insert_version([{TxId, Timemstamp, Value, Reader}|Rest], MyTxId, MyPrepTime, MyValue) -> 
%    [{TxId, Timemstamp, Value, Reader}|insert_version(Rest, MyTxId, MyPrepTime, MyValue)].


%add_to_commit_tab(WriteSet, TxCommitTime, Tab) ->
%    lists:foreach(fun({Key, Value}) ->
%            case ets:lookup(Tab, Key) of
%                [] ->
%                    true = ets:insert(Tab, {Key, [{TxCommitTime, Value}]});
%                [{Key, ValueList}] ->
%                    {RemainList, _} = lists:split(min(?NUM_VERSIONS,length(ValueList)), ValueList),
%                    true = ets:insert(Tab, {Key, [{TxCommitTime, Value}|RemainList]})
%            end
%    end, WriteSet).

%find_parts_for_name(Partitions) ->
%    Repls = antidote_config:get(to_repl),
%    [ReplNodes] = [L || {N, L} <- Repls, N == node()],
%    lists:foldl(fun(Node, List) ->
%                PartList = [P || {P, N} <- Partitions, N == Node],
%                PartList++List
%                end, [], ReplNodes).
