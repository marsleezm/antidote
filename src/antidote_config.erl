-module(antidote_config).

-export([load/1,
         set/2,
         get/1, get/2]).

%% ===================================================================
%% Public API
%% ===================================================================

load(File) ->
    FileName = case file:read_file_info(File) of
                    {ok, _} -> File;
                    {error, enoent} -> "../"++File;
                    {error, _R} -> File
               end,
    TermsList =
        case file:consult(FileName) of
              {ok, Terms} ->
                  Terms;
              {error, Reason} ->
                  io:format("Failed to parse config file ~s: ~p\n", [FileName, Reason])
          end,
    load_config(TermsList),
    case antidote_config:get(do_repl) of
        true ->
            lager:info("Will do replication");
        false ->
            lager:info("No replication")
    end,
    case antidote_config:get(do_cert) of
        true ->
            lager:info("Will do certification");
        false ->
            lager:info("No certification")
    end.

set(Key, Value) ->
    ok = application:set_env(antidote, Key, Value).

get(Key) ->
    case application:get_env(antidote, Key) of
        {ok, Value} ->
            Value;
        undefined ->
            io:format("Missing configuration key ~p", [Key])
    end.

get(Key, Default) ->
    case application:get_env(antidote, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

load_config([]) ->
    ok;
load_config([{Key, Value} | Rest]) ->
    ?MODULE:set(Key, Value),
    load_config(Rest);
load_config([ Other | Rest]) ->
    io:format("Ignoring non-tuple config value: ~p\n", [Other]),
    load_config(Rest).
