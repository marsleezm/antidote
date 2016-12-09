%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
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
-module(rubis_load).

-export([load/1, thread_load/4]).

-include("rubis.hrl").

-define(TIMEOUT, 20000).

%% ====================================================================
%% API
%% ====================================================================

load(_) ->
    lager:info("Start rubis load at ~w", [node()]),
    random:seed(now()), %now()),
    %% Choose the node using our ID as a modulus

    {_, PartList, ReplList, _} =  hash_fun:get_hash_fun(), %gen_server:call({global, MyTxServer}, {get_hash_fun}),
    %lager:info("Part list is ~w, Replist is ~w", [PartList, ReplList]),

    %% Generate kinda hash function and distrbution
    AllNodes = [N || {N, _} <- PartList],
    MyNode = node(),
    [M] = [L || {N, L} <- ReplList, N == MyNode ],
    MyRepIds = get_indexes(M, AllNodes),
    FullPartList = lists:flatten([L || {_, L} <- PartList]),
    lager:warning("My rep Ids are ~w, All nodes are ~w, FullPartlist is ~w", [MyRepIds, AllNodes, FullPartList]),
    HashLength = length(FullPartList),
    MyReps = lists:map(fun(Index) ->  Name = lists:nth(Index, AllNodes),  {Index, {rep, get_rep_name(MyNode, Name)}} end, MyRepIds),
    DcId = index(MyNode, AllNodes),
    %NumNodes = length(AllNodes),

    %% Delete table
    lager:info("TargetNode is ~p, DcId is ~w, My Replica Ids are ~w",[MyNode, DcId, MyReps]),
    StartTime = os:timestamp(),
    try
        ets:delete(load_info)
    catch
        error:badarg ->  lager:warning("Ets table already deleted!")
    end,
    ets:new(load_info, [named_table, public, set]),
    
    case DcId of 1 ->
            CT = tpcc_tool:now_nsec() - 100000,
            P1 = get_partition("COMMIT_TIME", FullPartList, HashLength),
            clocksi_vnode:append_values(P1, [{"COMMIT_TIME", CT}], CT, self()),
            timer:sleep(500);
        _ ->
            timer:sleep(3000)
    end,

    %% Add commit time 
    Part1 = get_partition("COMMIT_TIME", FullPartList, HashLength),
    {ok, COMMIT_TIME} = clocksi_vnode:internal_read(Part1, "COMMIT_TIME", tx_utilities:create_tx_id(0)),
    %lager:info("Got commit, is ~w", [COMMIT_TIME]),
    ets:insert(load_info, {"COMMIT_TIME", COMMIT_TIME}),

    ets:insert(load_info, {active_items, 0}),
    load_config(),

    ToPopulateParts = [{DcId, server}|MyReps],
    lager:info("Populating categories/regions for node ~w", [MyNode]),
    {_, L} = lists:nth(DcId, PartList),
    MyTables = lists:foldl(fun(P, Acc) ->  Tab = clocksi_vnode:get_table(P), Acc++[Tab] end, [], L),
    populate_commons(MyTables, MyNode, PartList, ToPopulateParts),
    lists:foreach(fun({Id, Server}) -> 
        lager:info("Spawning for dc ~w", [Id]),
        Tables = case Server of 
                            server -> 
                                    {_, L} = lists:nth(DcId, PartList),
                                    lists:foldl(fun(P, Acc) ->  Tab = clocksi_vnode:get_table(P), Acc++[Tab] end, [], L);
                            {rep, S} -> data_repl_serv:get_table(S)
                 end,
        spawn(rubis_load, thread_load, [Id, Tables, PartList, self()]) end, ToPopulateParts),
    lists:foreach(fun(_) ->  receive {done, I, S} -> ok, lager:info("Receive a reply from ~w, ~w", [I, S]) end end, ToPopulateParts), 
    ets:delete(load_info),
    EndTime = os:timestamp(),
    lager:info("Population finished.. used ~w", [timer:now_diff(EndTime, StartTime)/1000000]).

load_config() ->
    ConfigFile = "./include/rubis_prop.config",
    TermsList =
        case file:consult(ConfigFile) of
              {ok, Terms} ->
                  Terms;
              {error, Reason} ->
                  lager:info("Failed to parse config file ~s: ~p\n", [ConfigFile, Reason])
          end,
    load_config(TermsList).

load_config([]) ->
    ok;
load_config([{Key, Value} | Rest]) ->
    ets:insert(load_info, {Key, Value}),
    load_config(Rest);
load_config([ Other | Rest]) ->
    io:format("Ignoring non-tuple config value: ~p\n", [Other]),
    load_config(Rest).

thread_load(MyNode, Server, PartList, Sup) ->
    lager:info("Populating users for node ~w", [MyNode]),

    SellerD = populate_comments(Server, MyNode, PartList),
    populate_users(Server, MyNode, PartList, SellerD), 

    lager:info("Populating items for node ~w", [MyNode]),
    {CategoryDict, RegionDict} = populate_items(Server, MyNode, PartList), 

    lager:info("Finished populating items"),

    [{_, NumOrgOldItems}] = ets:lookup(load_info, database_number_of_old_items), 
    [{_, NumActiveItems}] = ets:lookup(load_info, active_items),
    [{_, ReduceFactor}] = ets:lookup(load_info, reduce_factor),

    LastItemId = (NumOrgOldItems div ReduceFactor) + (NumActiveItems div ReduceFactor), 
    LastItemKey = rubis_tool:get_key(MyNode, lastitem), 
    put_to_node(Server, MyNode, PartList, LastItemKey, LastItemId),

    LastBidId = LastItemId * 5, 
    LastBidKey = rubis_tool:get_key(MyNode, lastbid), 
    put_to_node(Server, MyNode, PartList, LastBidKey, LastBidId),

    LastBuyNowKey = rubis_tool:get_key(MyNode, lastbuynow), 
    put_to_node(Server, MyNode, PartList, LastBuyNowKey, 0),

    dict:fold(fun(CatId, List, _) ->
            RemainList = lists:sublist(List, ?CATEGORY_NEW_ITEMS), 
            CategoryKey = rubis_tool:get_key({MyNode, CatId}, categorynewitems),
            %lager:info("Putting ~p to Server ~w", [CategoryKey, Server]),
            put_to_node(Server, MyNode, PartList, CategoryKey, RemainList)
            end, nothing, CategoryDict),

    dict:fold(fun(RegionId, List, _) ->
            RemainList = lists:sublist(List, ?REGION_NEW_ITEMS), 
            RegionKey = rubis_tool:get_key({MyNode, RegionId}, regionnewitems),
            %lager:info("Putting ~p to Server ~w", [RegionKey, Server]),
            put_to_node(Server, MyNode, PartList, RegionKey, RemainList)
            end, nothing, RegionDict),

    Sup ! {done, MyNode, Server},
    ok.

populate_commons(TxServer, MyNode, PartList, PopulateNodes) ->
    %% Loading categories
    CategoryFile = "./include/ebay_simple_categories.txt",
    {ok, Device} = file:open(CategoryFile, [read]),
    load_all(Device, category, 1, PartList, TxServer, MyNode, PopulateNodes), 
    file:close(Device),

    RegionFile = "./include/ebay_regions.txt",
    {ok, Device1} = file:open(RegionFile, [read]),
    load_all(Device1, region, 1, PartList, TxServer, MyNode, PopulateNodes), 
    file:close(Device1).
    %% Load regions
    %put_range_items(TxServer, 1, ?NB_MAX_ITEM, DcId, PartList).

load_all(Device, Type, Count, PartList, TxServer, MyNode, PopulateNodes) ->
    case io:get_line(Device, "") of
        eof -> 
            case Type of 
                category ->
                    ets:insert(load_info, {num_categories, Count-1});
                region ->
                    ets:insert(load_info, {num_regions, Count-1})
            end;
        Line0 ->
            Value = case Type of
                category ->
                    [V0|Num0] = re:split(Line0, "\\(", [{return, list}]),
                    [Num|_] = re:split(Num0, "\\)", [{return, list}]),
                    [{_, ReduceFactor}] = ets:lookup(load_info, reduce_factor),
                    ReducedNum = list_to_integer(Num) div ReduceFactor,
                    ets:update_counter(load_info, active_items, ReducedNum), 
                    lists:foreach(fun({N, _}) ->
                        ets:insert(load_info, {{N, Count}, ReducedNum})
                            end, PopulateNodes),
                    droplast(V0);
                region ->
                    Line0
            end,
            Key = rubis_tool:get_key(Count, Type),
            %lager:info("Loading Key ~p, value is ~p to ~p, my node is ~w, part list is ~w", [Key, Value, TxServer, MyNode, PartList]),
            put_to_node(TxServer, MyNode, PartList, Key, Value),
            load_all(Device, Type, Count+1, PartList, TxServer, MyNode, PopulateNodes)
    end.

populate_comments(TxServer, MyNode, PartList) ->
    %%% Populate comments 
    [{_, NumOrgUsers}] = ets:lookup(load_info, database_number_of_users), 
    [{_, ReduceFactor}] = ets:lookup(load_info, reduce_factor), 
    NumUsers = NumOrgUsers div ReduceFactor,
    [{_, CommentFinalLength0}] = ets:lookup(load_info, database_comment_max_length), 
    CommentFinalLength = CommentFinalLength0 div 10,

    Seq = lists:seq(1, NumUsers),
    lists:foldl(fun(ItemId, SellerD) ->
                Seller = random:uniform(NumUsers), 
                FromUser = random:uniform(NumUsers),
                Rating = ItemId rem 5 - 2,
                {SellerD1, Ind} = case dict:find(Seller, SellerD) of
                        error -> {dict:store(Seller, {1, [{MyNode,ItemId}]}, SellerD),  1};
                        {ok, {Len, L}} -> {dict:store(Seller, {Len+1, [{MyNode,ItemId}|L]}, SellerD), Len+1}
                     end,
                CommentKey = rubis_tool:get_key({{MyNode, Seller}, Ind}, comment),
                Now = rubis_tool:now_nsec(),
                CommentObj = rubis_tool:create_comment({MyNode, FromUser}, {MyNode, Seller}, CommentFinalLength, ItemId, Rating, Now),
                put_to_node(TxServer, MyNode, PartList, CommentKey, CommentObj),
                SellerD1
            end, dict:new(), Seq).
    
populate_users(TxServer, MyNode, PartList, SellerD)->
    [{_, NumOrgUsers}] = ets:lookup(load_info, database_number_of_users), 
    [{_, ReduceFactor}] = ets:lookup(load_info, reduce_factor), 
    [{_, NumRegions}] = ets:lookup(load_info, num_regions), 
    NumUsers = NumOrgUsers div ReduceFactor,
    Now = rubis_tool:now_nsec(),
    Seq = lists:seq(1, NumUsers),
    lists:foreach(fun(Index) ->
                    UserId = {MyNode, Index},
                    UserKey = rubis_tool:get_key(UserId, user),
                    User = rubis_tool:create_user(Index, Now, 0, 0, (Index rem NumRegions) + 1),
                    {NumComments, SellingItems} = case dict:find(Index, SellerD) of
                                    error -> {0, []};
                                    {ok, {Len, Is}} -> {Len, Is} 
                                   end,
                    User1 = User#user{u_num_comments=NumComments, u_sellings=SellingItems}, 
                    put_to_node(TxServer, MyNode, PartList, UserKey, User1)
                end, Seq).

populate_items(TxServer, MyNode, PartList) ->
    random:seed({1, 1, MyNode}),
    [{_, NumOrgOldItems}] = ets:lookup(load_info, database_number_of_old_items), 
    [{_, NumActiveItems}] = ets:lookup(load_info, active_items),
    [{_, ReduceFactor}] = ets:lookup(load_info, reduce_factor), 
    [{_, Duration}] = ets:lookup(load_info, max_duration), 

    [{_, ReserverPrice}] = ets:lookup(load_info, database_percentage_of_items_with_reserve_price), 
    [{_, BuyNowItems}] = ets:lookup(load_info, database_percentage_of_buy_now_items), 
    [{_, UniqueItems}] = ets:lookup(load_info, database_percentage_of_unique_items), 
    [{_, MaxQuantity}] = ets:lookup(load_info, database_max_quantity_for_multiple_items), 

    [{_, NumOrgUsers}] = ets:lookup(load_info, database_number_of_users), 
    [{_, CommentLength0}] = ets:lookup(load_info, database_item_description_length),
    CommentLength = CommentLength0 div 10,
    %[{_, NumOrgMaxBids}] = ets:lookup(load_info, database_max_bids_per_item), 
    %[{_, NumRegions}] = ets:lookup(load_info, num_regions), 
    [{_, NumCategories}] = ets:lookup(load_info, num_categories), 
    [{_, NumRegions}] = ets:lookup(load_info, num_regions), 
    NumOldItems = NumOrgOldItems div ReduceFactor,
    NumUsers = NumOrgUsers div ReduceFactor,
    %NumMaxBids = NumOrgMaxBids div 4,

    lager:info("Generating ~w old items and ~w new items", [NumOldItems, NumActiveItems]),
    TotalItems = NumOldItems + NumActiveItems,
    Seq = lists:seq(1, TotalItems),

    PerctOldItems = NumOldItems / 100, 
    PerctActiveItems = NumActiveItems / 100, 
    {CD0, RD0} = lists:foldl(fun(Index, {CD, RD}) ->
            InitialPrice = random:uniform(5000),
            {Dur, RPrice, BuyNowPrice, Qty, CategoryId} = case Index =< NumOldItems of
                true -> %% Is old items
                      ReservePrice = case Index =< ReserverPrice * PerctOldItems of
                          true -> InitialPrice + random:uniform(1000); 
                          false -> 0
                      end,
                      BP = case Index =< BuyNowItems * PerctOldItems of
                          true -> InitialPrice + ReservePrice + random:uniform(1000);
                          false -> 0
                      end,
                      Q = case Index =< UniqueItems * PerctOldItems of
                          true -> 1; 
                          false -> random:uniform(MaxQuantity)
                      end,
                      {-Duration, ReservePrice, BP, Q, Index rem NumCategories + 1 }; 
                false ->
                      ReservePrice = case Index-NumOldItems =< ReserverPrice * PerctActiveItems of
                          true -> InitialPrice + random:uniform(1000); 
                          false -> 0
                      end,
                      BP = case Index-NumOldItems =< BuyNowItems * PerctActiveItems of
                          true -> InitialPrice + ReservePrice + random:uniform(1000);
                          false -> 0
                      end,
                      Q = case Index-NumOldItems =< UniqueItems * PerctActiveItems of
                          true -> 1; 
                          false -> random:uniform(MaxQuantity)
                      end,
                      CatId = find_categories(MyNode, Index, NumCategories),
                      {Duration, ReservePrice, BP, Q, CatId}
            end, 
            SellerId = {MyNode, random:uniform(NumUsers)},
            ItemId = {MyNode, Index},
            Name = Index,
            Now = rubis_tool:now_nsec(),
            Item = rubis_tool:create_item(Name, CommentLength, InitialPrice, Qty, RPrice, BuyNowPrice,
              Now, Now+Dur, SellerId, CategoryId),
            ItemKey = rubis_tool:get_key(ItemId, item),
            put_to_node(TxServer, MyNode, PartList, ItemKey, Item),

            %%% Populate 5 bids for each item
            BidSeq = lists:seq((Index-1)*5+1, (Index-1)*5+5),
            lists:foreach(fun(Int) ->
                    AddBid = random:uniform(10),
                    BidKey = rubis_tool:get_key({MyNode, Int}, bid),
                    %%% May need to change here!!!!
                    Bid = rubis_tool:create_bid({MyNode, random:uniform(NumUsers)}, ItemId, random:uniform(Qty), 
                            InitialPrice+AddBid, InitialPrice+AddBid*2, Now),
                    put_to_node(TxServer, MyNode, PartList, BidKey, Bid)
                    end, BidSeq),
            case Index =< NumOldItems of
                true -> {CD, RD};
                false -> {dict:append(CategoryId, ItemId, CD), 
                    dict:append(random:uniform(NumRegions), ItemId, RD)}
            end
            end, {dict:new(), dict:new()}, Seq),

    CD1 = lists:foldl(fun(V, D) ->
                        case dict:find(V, D) of
                            error -> dict:store(V, empty, D);
                            _ -> D
                        end
                        end, CD0, lists:seq(1,NumCategories)),
    RD1 = lists:foldl(fun(V, D) ->
                        case dict:find(V, D) of
                            error -> dict:store(V, empty, D);
                            _ -> D
                        end
                        end, RD0, lists:seq(1,NumRegions)),
    {CD1, RD1}.

find_categories(MyNode, Index, CatNum) ->
    C = Index rem CatNum + 1,
    [{_, Num}] = ets:lookup(load_info, {MyNode, C}),
    case Num of 0 -> 
                     find_categories(MyNode, Index+1, CatNum);
                _ -> ets:insert(load_info, {{MyNode, C}, Num-1}),
                     C
    end.

get_partition(Key, PartList, HashLength) ->
    Num = crypto:bytes_to_integer(erlang:md5(Key)) rem HashLength +1,
    lists:nth(Num, PartList).
    
put_to_node(Tabs, _DcId, _PartList, Key, Value) ->
    [{"COMMIT_TIME", CommitTime}] = ets:lookup(load_info, "COMMIT_TIME"),
    case is_list(Tabs) of
        true -> %% Is clocksi table
            Index = crypto:bytes_to_integer(erlang:md5(Key)) rem length(Tabs) + 1,
            Tab = lists:nth(Index, Tabs),
            %lager:info("Putting ~p ~p to ~w", [Key, Value, Tab]),
            ets:insert(Tab, {Key, [{CommitTime, Value}]});
        false ->
            ets:insert(Tabs, {Key, [{CommitTime, Value}]})
    end.

%defer_put(DcId, PartList, Key, Value, WriteSet) ->
%    {_, L} = lists:nth(DcId, PartList),
%    Index = crypto:bytes_to_integer(erlang:md5(Key)) rem length(L) + 1,
%    dict:append(Index, {Key, Value}, WriteSet).

%multi_put(TxServer, DcId, PartList, WriteSet) ->
%    {_, L} = lists:nth(DcId, PartList%),
%    DictList = dict:to_list(WriteSet),
%    [{"COMMIT_TIME", CommitTime}] = ets:lookup(load_info, "COMMIT_TIME"),
%    lists:foreach(fun({Index, KeyValues}) ->
%            Part = lists:nth(Index, L),    
%            put(TxServer, Part, KeyValues, CommitTime)
%            end, DictList).              

%put(TxServer, Part, KeyValues, CommitTime) ->
%    %lager:info("Single Puting [~p, ~p] to ~w", [Key, Value, Part]),
%    case TxServer of
%        server ->
%            {ok, _} = clocksi_vnode:append_values(Part, KeyValues, CommitTime, self());
%        {rep, S} ->
%            ok = data_repl_serv:append_values(S, KeyValues, CommitTime)
%    end.

index(Elem, L) ->
    index(Elem, L, 1).

index(_, [], _) ->
    -1;
index(E, [E|_], N) ->
    N;
index(E, [_|L], N) ->
    index(E, L, N+1).

get_indexes(PL, List) ->
    %lager:info("Trying to get index: PL ~w, List ~w", [PL, List]),
    [index(X, List) || X <- PL ].

get_rep_name(Target, Rep) ->
    list_to_atom(atom_to_list(Target)++"repl"++atom_to_list(Rep)).

droplast(L)->
    droplast(L, []).

droplast([_], Acc) ->
    lists:reverse(Acc);
droplast([X|H], Acc) ->
    droplast(H, [X|Acc]).
