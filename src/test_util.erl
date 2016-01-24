-module(test_util).


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([delete_1/2, delete_2/2,
         lookup_1/2, lookup_2/2, get_my_range/4,
         check_node/1, check_list/1, test_hash/2,
         pass1/1, pass2/1]). %, set1/2, set2/2]).

%set1(N, S) ->
%    Seq = lists:seq(1, S),
%    SeqN = lists:seq(1, N),
%    Set1 = lists:foldl(fun(M,S) -> sets:add_element(M,S) end, sets:new(), Seq),
%    T = os:timestamp(),

%    lists:foldl(fun(_, S) ->
%            K = random:uniform(5),
%            S1 = sets:add_element(T, S1),
%            end, Seq),
%    Diff = timer:now_diff(os:timestamp(), T),
%    io:format("Diff is ~w ~n", [Diff]).

test_hash(N, StrLen) ->
    Key = random_string(StrLen),
    PartList = [[100, 111,333], [444, 22333], [222, 3333], [1122, 1111]],
    Seq = lists:seq(1, N),
    T = os:timestamp(),
    lists:foreach(fun(_) ->
            L = lists:nth(2, PartList),
            crypto:bytes_to_integer(erlang:md5(Key)) rem length(L) + 1
            end, Seq),
    Diff = timer:now_diff(os:timestamp(), T),
    io:format("Diff is ~w ~n", [Diff]).

get_my_range(Total, Start, NumIds, MyId) ->
    Remainder = Total rem NumIds,
    Div = (Total-Remainder)/NumIds,
    First = ((MyId-1) * Div) + Start,
    Last = case MyId of
                NumIds ->
                  Div + Remainder + First - 1;
                _ ->
                  Div + First -1
               end,
    {First, Last}.

check_node(Times) ->
    Seq = lists:seq(1, Times),
    P = self(),
    A = now_nsec(),
    lists:map(fun(_) ->
           node(P) == node()
            end, Seq),
    B = now_nsec(),
    io:format("Used ~w", [B-A]).
    
check_list(Times) ->
    Seq = lists:seq(1, Times),
    P = self(),
    A = now_nsec(),
    lists:map(fun(_) ->
        L = pid_to_list(P),
        lists:nth(2, L) == 48 
            end, Seq),
    B = now_nsec(),
    io:format("Used ~w", [B-A]).


delete_1(Len, Elem) ->
    Seq = lists:seq(1, Len),
    L = [ {X, X*5, X*6}  || X <- Seq],
    A = now_nsec(),
    _Result = df1(Elem, L),
    B = now_nsec(),
    io:format("Used ~w", [B-A]).
    %io:format("Used ~w, List is ~p", [B-A, Result]).

df1(Elem, [{_, _, Elem}|Rest]) ->
    Rest;
df1(Elem, [{A, B, E}|Rest]) ->
    [{A, B, E}|df1(Elem, Rest)].

delete_2(Len, Elem) ->
    Seq = lists:seq(1, Len),
    L = [ {X, X*5, X*6}  || X <- Seq],
    A = now_nsec(),
    _Result = df2(Elem, L, []),
    B = now_nsec(),
    io:format("Used ~w", [B-A]).

df2(Elem, [{_, _, Elem}|Rest], Start) ->
    lists:reverse(Start)++Rest;
df2(Elem, [{A, B, E}|Rest], Start) ->
    df2(Elem, Rest, [{A,B,E}|Start]).

now_nsec() ->
    {A, B, C} = now(),
    (A * 1000000 + B) * 1000000 + C. 

lookup_1(Times, NumItems) ->
    Seq2 = lists:seq(1, Times),
    ets:new(table, [public, set, named_table]),
    Dict = init(NumItems),
    RandSeq = [random:uniform(10) || _ <- Seq2] ,  
    A = now_nsec(),
    lists:foldl(fun(X, D) ->
             check1(X, 8, D)
             end, Dict, RandSeq),
            B = now_nsec(),
    io:format("Used ~w ~n", [B-A]),
    ets:delete(table).

lookup_2(Times, NumItems) ->
    Seq2 = lists:seq(1, Times),
    ets:new(table, [public, set, named_table]),
    _ = init(NumItems),
    RandSeq = [random:uniform(10) || _ <- Seq2] ,  
    A = now_nsec(),
    lists:foreach(fun(X) ->
             check2(X, 8)
             end, RandSeq),
    B = now_nsec(),
    io:format("Used ~w ~n", [B-A]),
    ets:delete(table).

pass1(N) ->
    Seq = lists:seq(1, N),
    A = now_nsec(),
    lists:foreach(fun(_) ->
          fun1(1,2,3,4,5,6,7,8,9,10) end, Seq),
    B = now_nsec(),
    io:format("Used ~w ~n", [B-A]).

pass2(N) ->
    ets:new(table, [public, set, named_table]),
    ets:insert(table, {2,3,4,5,6,7,8,9}),
    Seq = lists:seq(1, N),
    A = now_nsec(),
    lists:foreach(fun(_) ->
          fun2(1) end, Seq),
    B = now_nsec(),
    io:format("Used ~w ~n", [B-A]),
    ets:delete(table).

fun1(_,_,_,_,_,_,_,_,_,_) ->
    ok.

fun2(_) ->
    ets:lookup(table,2),
    ok.


check1(Key, Times, Dict) ->
    Seq = lists:seq(1, Times),
    lists:foreach(fun(_) -> {ok, {V1, V2, V3}}= dict:find(Key, Dict),
                    dict:store(Key, {V1, V2, V3}, Dict) end, Seq),
    ets:lookup(table, Key),
    Dict.

check2(Key, Times) ->
    Seq = lists:seq(1, Times),
    lists:foreach(fun(_) -> [{Key, {V1, V2, V3, V4}}]=  ets:lookup(table, Key),
                    ets:insert(table, {Key, {V1, V2, V3, V4}}) end, Seq).
    


init(NumItems) ->
    Seq = lists:seq(1, NumItems),
    lists:foreach(fun(X) -> ets:insert(table, {X, {X, 2*X, 3*X, "123456789014567890123457890123456789012345678901234567890123456789012345678901234567890"}}) end, Seq),
    lists:foldl(fun(X, D) ->     
                        dict:store(X, {X, 2*X, 3*X}, D) end, dict:new(), Seq).

random_string(Len) ->
    Chrs = list_to_tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"),
    ChrsSize = size(Chrs),
    F = fun(_, R) -> [element(random:uniform(ChrsSize), Chrs) | R] end,
    lists:foldl(F, "", lists:seq(1, Len)).

