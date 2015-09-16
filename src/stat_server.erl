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
-module(stat_server).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

%% States
-export([send_stat/2, get_stat/1]).

%% Spawn

-record(state, {partition :: non_neg_integer(),
        read_count :: non_neg_integer(),
        prepare_count :: non_neg_integer(),
        read_stat :: [],
        prepare_stat :: [] 
        }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({global, get_statserv_name()},
             ?MODULE, [], []).

send_stat(ReadStat, PrepareStat) ->
    gen_server:cast({global, get_statserv_name()}, {send_stat, ReadStat, PrepareStat}).

get_stat(Node) ->
    gen_server:call({global, get_statserv_name(Node)}, {get_stat}).

%%%===================================================================
%%% Internal
%%%===================================================================


init([]) ->
    {ok, #state{read_count=0,
                prepare_count=0,
                read_stat=[],
                prepare_stat=[]}}.

handle_call({get_stat}, _Sender, SD0=#state{read_stat=ReadStat, read_count=ReadCount, 
            prepare_count=PrepareCount, prepare_stat=PrepareStat}) ->
    lager:info("PStat is ~w, ~w, RStat is ~w, ~w", [PrepareCount, PrepareStat, ReadCount, ReadStat]),
    {reply, {PrepareCount, PrepareStat, ReadCount, ReadStat}, SD0};

handle_call({go_down},_Sender,SD0) ->
    {stop,shutdown,ok,SD0}.

handle_cast({send_stat, ReadL, PrepareL}, 
	    SD0=#state{read_count=ReadCount, read_stat=ReadStat, 
            prepare_count=PrepareCount, prepare_stat=PrepareStat}) ->
    {ReadCount1, ReadStat1} = case ReadL of
                                [] ->
                                    {ReadCount, ReadStat};
                                _ ->            
                                    lager:info("ReadL ~w", [ReadL]),
                                    {ReadCount+1, increment_all(ReadL, ReadStat, [])}
                              end,
    {PrepareCount1, PrepareStat1} = case PrepareL of
                                        [] ->
                                            {PrepareCount, PrepareStat};
                                        _ ->            
                                            lager:info("PrepareL ~w", [PrepareL]),
                                            {PrepareCount+1, increment_all(PrepareL, PrepareStat, [])}
                                    end,
    {noreply, SD0#state{prepare_count=PrepareCount1, prepare_stat=PrepareStat1, 
                        read_count=ReadCount1, read_stat=ReadStat1}};

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

get_statserv_name() ->
    list_to_atom(atom_to_list(stat_server)++atom_to_list(node())).

get_statserv_name(Node) ->
    list_to_atom(atom_to_list(stat_server)++atom_to_list(Node)).

increment_all([], [], LAcc) ->
    lists:reverse(LAcc);
increment_all(Stat, [], _) ->
    Stat;
increment_all([F|T], [LF|LT], LAcc) ->
    increment_all(T, LT, [LF+F|LAcc]).
     
    
-ifdef(TEST).

increment_test() ->
    A = increment_all([2,3,4,5], [3,1,2,2], []),
    ?assertEqual(A, [5,4,6,7]). 

-endif.

