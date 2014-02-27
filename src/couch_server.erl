% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_server).
-behaviour(gen_server).

-export([open/2,create/2,delete/2,get_version/0,get_uuid/0]).
-export([all_databases/0, all_databases/2]).
-export([dir/0, dir/1]).
-export([db2couch/1]).

-export([start_link/0]).
-export([init/1, handle_call/3,
         handle_cast/2,code_change/3,handle_info/2,terminate/2]).


-export([dev_start/0,is_admin/2, get_stats/0]).
-export([close_lru/0]).

-include("couch_db.hrl").

-record(server,{
    root_dir = [],
    dbname_regexp,
    max_dbs_open=100,
    dbs_open=0,
    start_time="",
    lru = couch_lru:new()
    }).

-ifdef(crypto_compat).
-define(SHA(Data), crypto:sha(Data)).
-else.
-define(SHA(Data), crypto:hash(sha, Data)).
-endif.

dev_start() ->
    couch:stop(),
    up_to_date = make:all([load, debug_info]),
    couch:start().

get_version() ->
    Apps = application:loaded_applications(),
    case lists:keysearch(couch, 1, Apps) of
    {value, {_, _, Vsn}} ->
        Vsn;
    false ->
        "0.0.0"
    end.

get_uuid() ->
    case couch_app:get_env(uuid) of
        undefined ->
            UUID = couch_uuids:random(),
            application:set_env(uuid, UUID);
        UUID ->
            couch_util:to_binary(UUID)
    end.

get_stats() ->
    {ok, #server{start_time=Time,dbs_open=Open}} =
            gen_server:call(couch_server, get_server),
    [{start_time, ?l2b(Time)}, {dbs_open, Open}].

start_link() ->
    gen_server:start_link({local, couch_server}, couch_server, [], []).


open(DbName, Options) ->
    Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
    case ets:lookup(couch_dbs, DbName) of
    [#db{fd=Fd, fd_monitor=Lock} = Db] when Lock =/= locked ->
        update_lru(DbName, Options),
        {ok, Db#db{user_ctx=Ctx, fd_monitor=erlang:monitor(process,Fd)}};
    _ ->
        Timeout = couch_util:get_value(timeout, Options, infinity),
        case gen_server:call(couch_server, {open, DbName, Options}, Timeout) of
        {ok, #db{fd=Fd} = Db} ->
            update_lru(DbName, Options),
            {ok, Db#db{user_ctx=Ctx, fd_monitor=erlang:monitor(process,Fd)}};
        Error ->
            Error
        end
    end.

update_lru(DbName, Options) ->
    case lists:member(sys_db, Options) of
        false -> gen_server:cast(couch_server, {update_lru, DbName});
        true -> ok
    end.

close_lru() ->
    gen_server:call(couch_server, close_lru).

create(DbName, Options) ->
    case gen_server:call(couch_server, {create, DbName, Options}, infinity) of
    {ok, #db{fd=Fd} = Db} ->
        Ctx = couch_util:get_value(user_ctx, Options, #user_ctx{}),
        {ok, Db#db{user_ctx=Ctx, fd_monitor=erlang:monitor(process,Fd)}};
    Error ->
        Error
    end.

delete(DbName, Options) ->
    gen_server:call(couch_server, {delete, DbName, Options}, infinity).


check_dbname(#server{dbname_regexp=RegExp}, DbName) ->
    case re:run(DbName, RegExp, [{capture, none}]) of
        nomatch ->
            {error, illegal_database_name, DbName};
        match ->
            ok
    end.

default_dir() ->
    Name = lists:concat(["opencouch.", node()]),
    filename:absname(Name).

dir() ->
    couch_app:get_env(dir, default_dir()).

dir(Fname) ->
    filename:join([dir(), Fname]).

is_admin(User, ClearPwd) ->
    case config:get("admins", User) of
    "-hashed-" ++ HashedPwdAndSalt ->
        [HashedPwd, Salt] = string:tokens(HashedPwdAndSalt, ","),
        couch_util:to_hex(?SHA(ClearPwd ++ Salt)) == HashedPwd;
    _Else ->
        false
    end.

db2couch(DbName) ->
    dir(lists:concat([DbName, ".couch"])).

init([]) ->
    RootDir = dir(),
    MaxDbsOpen = couch_app:get_env(max_dbs_open, 100),
    ok = couch_file:init_delete_dir(RootDir),
    {ok, RegExp} = re:compile(
        "^[a-z][a-z0-9\\_\\$()\\+\\-\\/]*" % use the stock CouchDB regex
        "(\\.[0-9]{10,})?$" % but allow an optional shard timestamp at the end
    ),
    ets:new(couch_dbs, [set, protected, named_table, {keypos, #db.name}]),
    process_flag(trap_exit, true),
    {ok, #server{root_dir=RootDir,
                dbname_regexp=RegExp,
                max_dbs_open=MaxDbsOpen,
                start_time=couch_util:rfc1123_date()}}.

terminate(_Reason, _Srv) ->
    ets:foldl(fun(#db{main_pid=Pid}, _) -> couch_util:shutdown_sync(Pid) end,
        nil, couch_dbs),
    ok.

all_databases() ->
    {ok, DbList} = all_databases(
        fun(DbName, Acc) -> {ok, [DbName | Acc]} end, []),
    {ok, lists:usort(DbList)}.

all_databases(Fun, Acc0) ->
    {ok, #server{root_dir=Root}} = gen_server:call(couch_server, get_server),
    NormRoot = couch_util:normpath(Root),
    FinalAcc = try
        filelib:fold_files(Root,
                           "^[a-z0-9\\_\\$()\\+\\-]*" % stock CouchDB name regex
                           "(\\.[0-9]{10,})?"         % optional shard timestamp
                           "\\.couch$",               % filename extension
                           true,  fun(Filename, AccIn) ->
                    NormFilename = couch_util:normpath(Filename),
                    RelativeFilename = case NormFilename -- NormRoot of
                        [$/ | RelFilename] -> RelFilename;
                        RelFilename -> RelFilename
                    end,
                    case Fun(?l2b(filename:rootname(RelativeFilename,
                                                    ".couch")), AccIn) of
                        {ok, NewAcc} -> NewAcc;
                        {stop, NewAcc} -> throw({stop, Fun, NewAcc})
                    end
            end, Acc0)
    catch throw:{stop, Fun, Acc1} ->
            Acc1
    end,
    {ok, FinalAcc}.


make_room(Server, Options) ->
    case lists:member(sys_db, Options) of
        false -> maybe_close_lru_db(Server);
        true -> {ok, Server}
    end.

maybe_close_lru_db(#server{dbs_open=NumOpen, max_dbs_open=MaxOpen}=Server)
        when NumOpen < MaxOpen ->
    {ok, Server};
maybe_close_lru_db(#server{lru=Lru}=Server) ->
    try
        {ok, db_closed(Server#server{lru = couch_lru:close(Lru)}, [])}
    catch error:all_dbs_active ->
        {error, all_dbs_active}
    end.

open_async(Server, From, DbName, Filepath, Options) ->
    Parent = self(),
    put({async_open, DbName}, now()),
    Opener = spawn_link(fun() ->
        Res = couch_db:start_link(DbName, Filepath, Options),
        case {Res, lists:member(create, Options)} of
            {{ok, _Db}, true} ->
                couch_db_update_notifier:notify({created, DbName});
            _ ->
                ok
        end,
        gen_server:call(Parent, {open_result, DbName, Res}, infinity),
        unlink(Parent)
    end),
    ReqType = case lists:member(create, Options) of
        true -> create;
        false -> open
    end,
    % icky hack of field values - compactor_pid used to store clients
    % and fd used for opening request info
    true = ets:insert(couch_dbs, #db{
        name = DbName,
        fd = ReqType,
        main_pid = Opener,
        compactor_pid = [From],
        fd_monitor = locked,
        options = Options
    }),
    db_opened(Server, Options).

handle_call(close_lru, _From, #server{lru=Lru} = Server) ->
    try
        {reply, ok, db_closed(Server#server{lru = couch_lru:close(Lru)}, [])}
    catch error:all_dbs_active ->
        {reply, {error, all_dbs_active}, Server}
    end;
handle_call(open_dbs_count, _From, Server) ->
    {reply, Server#server.dbs_open, Server};
handle_call({set_max_dbs_open, Max}, _From, Server) ->
    {reply, ok, Server#server{max_dbs_open=Max}};
handle_call(get_server, _From, Server) ->
    {reply, {ok, Server}, Server};
handle_call({open_result, DbName, {ok, Db}}, _From, Server) ->
    link(Db#db.main_pid),
    case erase({async_open, DbName}) of undefined -> ok; T0 ->
        lager:info("needed ~p ms to open new ~s", [timer:now_diff(now(),T0)/1000,
            DbName])
    end,
    % icky hack of field values - compactor_pid used to store clients
    % and fd used to possibly store a creation request
    [#db{fd=ReqType, compactor_pid=Froms}] = ets:lookup(couch_dbs, DbName),
    [gen_server:reply(From, {ok, Db}) || From <- Froms],
    % Cancel the creation request if it exists.
    case ReqType of
        {create, DbName, _Filepath, _Options, CrFrom} ->
            gen_server:reply(CrFrom, file_exists);
        _ ->
            ok
    end,
    true = ets:insert(couch_dbs, Db),
    Lru = case couch_db:is_system_db(Db) of
        false ->
            Stat = {couchdb, open_databases},
            couch_stats_collector:track_process_count(Db#db.main_pid, Stat),
            couch_lru:insert(DbName, Server#server.lru);
        true ->
            Server#server.lru
    end,
    {reply, ok, Server#server{lru = Lru}};
handle_call({open_result, DbName, {error, eexist}}, From, Server) ->
    handle_call({open_result, DbName, file_exists}, From, Server);
handle_call({open_result, DbName, Error}, _From, Server) ->
    % icky hack of field values - compactor_pid used to store clients
    [#db{fd=ReqType, compactor_pid=Froms}=Db] = ets:lookup(couch_dbs, DbName),
    [gen_server:reply(From, Error) || From <- Froms],
    lager:info("open_result error ~p for ~s", [Error, DbName]),
    true = ets:delete(couch_dbs, DbName),
    NewServer = case ReqType of
        {create, DbName, Filepath, Options, CrFrom} ->
            open_async(Server, CrFrom, DbName, Filepath, Options);
        _ ->
            Server
    end,
    {reply, ok, db_closed(NewServer, Db#db.options)};
handle_call({open, DbName, Options}, From, Server) ->
    case ets:lookup(couch_dbs, DbName) of
    [] ->
        DbNameList = binary_to_list(DbName),
        case check_dbname(Server, DbNameList) of
        ok ->
            case make_room(Server, Options) of
            {ok, Server2} ->
                Filepath = db2couch(DbNameList),
                {noreply, open_async(Server2, From, DbName, Filepath, Options)};
            CloseError ->
                {reply, CloseError, Server}
            end;
        Error ->
            {reply, Error, Server}
        end;
    [#db{compactor_pid = Froms} = Db] when is_list(Froms) ->
        % icky hack of field values - compactor_pid used to store clients
        true = ets:insert(couch_dbs, Db#db{compactor_pid = [From|Froms]}),
        if length(Froms) =< 10 -> ok; true ->
            Fmt = "~b clients waiting to open db ~s",
            lager:info(Fmt, [length(Froms), DbName])
        end,
        {noreply, Server};
    [#db{} = Db] ->
        {reply, {ok, Db}, Server}
    end;
handle_call({create, DbName, Options}, From, Server) ->
    DbNameList = binary_to_list(DbName),
    Filepath = db2couch(DbNameList),
    case check_dbname(Server, DbNameList) of
    ok ->
        case ets:lookup(couch_dbs, DbName) of
        [] ->
            case make_room(Server, Options) of
            {ok, Server2} ->
                {noreply, open_async(Server2, From, DbName, Filepath,
                        [create | Options])};
            CloseError ->
                {reply, CloseError, Server}
            end;
        [#db{fd=open}=Db] ->
            % We're trying to create a database while someone is in
            % the middle of trying to open it. We allow one creator
            % to wait while we figure out if it'll succeed.
            % icky hack of field values - fd used to store create request
            CrOptions = [create | Options],
            NewDb = Db#db{fd={create, DbName, Filepath, CrOptions, From}},
            true = ets:insert(couch_dbs, NewDb),
            {noreply, Server};
        [_AlreadyRunningDb] ->
            {reply, file_exists, Server}
        end;
    Error ->
        {reply, Error, Server}
    end;
handle_call({delete, DbName, Options}, _From, Server) ->
    DbNameList = binary_to_list(DbName),
    case check_dbname(Server, DbNameList) of
    ok ->
        FullFilepath = db2couch(DbNameList),
        Server2 =
        case ets:lookup(couch_dbs, DbName) of
        [] -> Server;
        [#db{main_pid=Pid, compactor_pid=Froms} = Db] when is_list(Froms) ->
            % icky hack of field values - compactor_pid used to store clients
            true = ets:delete(couch_dbs, DbName),
            exit(Pid, kill),
            [gen_server:reply(F, not_found) || F <- Froms],
            db_closed(Server, Db#db.options);
        [#db{main_pid=Pid} = Db] ->
            true = ets:delete(couch_dbs, DbName),
            exit(Pid, kill),
            db_closed(Server, Db#db.options)
        end,

        %% Delete any leftover compaction files. If we don't do this a
        %% subsequent request for this DB will try to open them to use
        %% as a recovery.
        lists:foreach(fun(Ext) ->
            couch_file:delete(Server#server.root_dir, FullFilepath ++ Ext)
        end, [".compact", ".compact.data", ".compact.meta"]),
        couch_file:delete(Server#server.root_dir, FullFilepath ++ ".compact"),

        Async = not lists:member(sync, Options),

        case couch_file:delete(Server#server.root_dir, FullFilepath, Async) of
        ok ->
            couch_db_update_notifier:notify({deleted, DbName}),
            {reply, ok, Server2};
        {error, enoent} ->
            {reply, not_found, Server2};
        Else ->
            {reply, Else, Server2}
        end;
    Error ->
        {reply, Error, Server}
    end;
handle_call({db_updated, #db{name = DbName} = Db}, _From, Server) ->
    true = ets:insert(couch_dbs, Db),
    Lru = case couch_db:is_system_db(Db) of
        false -> couch_lru:update(DbName, Server#server.lru);
        true -> Server#server.lru
    end,
    {reply, ok, Server#server{lru = Lru}}.

handle_cast({update_lru, DbName}, #server{lru = Lru} = Server) ->
    {noreply, Server#server{lru = couch_lru:update(DbName, Lru)}};
handle_cast(Msg, Server) ->
    {stop, {unknown_cast_message, Msg}, Server}.

code_change(_, State, _) ->
    {ok, State}.

handle_info({'EXIT', Pid, Reason}, Server) ->
    case ets:match_object(couch_dbs, #db{main_pid=Pid, _='_'}) of
    [#db{name = DbName, compactor_pid=Froms} = Db] ->
        if Reason /= snappy_nif_not_loaded -> ok; true ->
            Msg = io_lib:format("To open the database `~s`, Apache CouchDB "
                "must be built with Erlang OTP R13B04 or higher.", [DbName]),
            lager:error(Msg, [])
        end,
        lager:info("db ~s died with reason ~p", [DbName, Reason]),
        % icky hack of field values - compactor_pid used to store clients
        if is_list(Froms) ->
            [gen_server:reply(From, Reason) || From <- Froms];
        true ->
            ok
        end,
        true = ets:delete(couch_dbs, DbName),
        {noreply, db_closed(Server, Db#db.options)};
    [] ->
        {noreply, Server}
    end;
handle_info(Info, Server) ->
    {stop, {unknown_message, Info}, Server}.

db_opened(Server, Options) ->
    case lists:member(sys_db, Options) of
        false -> Server#server{dbs_open=Server#server.dbs_open + 1};
        true -> Server
    end.

db_closed(Server, Options) ->
    case lists:member(sys_db, Options) of
        false -> Server#server{dbs_open=Server#server.dbs_open - 1};
        true -> Server
    end.
