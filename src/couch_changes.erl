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

-module(couch_changes).
-include("couch_db.hrl").

-export([
    handle_changes/3,
    get_changes_timeout/2,
    wait_db_updated/3,
    get_rest_db_updated/1,
    configure_filter/4,
    filter/3
]).

-export([changes_enumerator/2]).

% For the builtin filter _docs_ids, this is the maximum number
% of documents for which we trigger the optimized code path.
-define(MAX_DOC_IDS, 100).

-record(changes_acc, {
    db,
    seq,
    prepend,
    filter,
    callback,
    user_acc,
    resp_type,
    limit,
    include_docs,
    conflicts,
    timeout,
    timeout_fun
}).

%% @type Req -> #httpd{} | {json_req, JsonObj()}
handle_changes(Args1, Req, Db0) ->
    #changes_args{
        style = Style,
        filter = FilterName,
        feed = Feed,
        dir = Dir,
        since = Since
    } = Args1,
    Filter = configure_filter(FilterName, Style, Req, Db0),
    Args = Args1#changes_args{filter_fun = Filter},
    Start = fun() ->
        {ok, Db} = couch_db:reopen(Db0),
        StartSeq = case Dir of
        rev ->
            couch_db:get_update_seq(Db);
        fwd ->
            Since
        end,
        {Db, StartSeq}
    end,
    % begin timer to deal with heartbeat when filter function fails
    case Args#changes_args.heartbeat of
    undefined ->
        erlang:erase(last_changes_heartbeat);
    Val when is_integer(Val); Val =:= true ->
        put(last_changes_heartbeat, now())
    end,

    case lists:member(Feed, ["continuous", "longpoll", "eventsource"]) of
    true ->
        fun(CallbackAcc) ->
            {Callback, UserAcc} = get_callback_acc(CallbackAcc),
            Self = self(),
            {ok, Notify} = couch_db_update_notifier:start_link(
                fun({_, DbName}) when  Db0#db.name == DbName ->
                    Self ! db_updated;
                (_) ->
                    ok
                end
            ),
            {Db, StartSeq} = Start(),
            UserAcc2 = start_sending_changes(Callback, UserAcc, Feed),
            {Timeout, TimeoutFun} = get_changes_timeout(Args, Callback),
            Acc0 = build_acc(Args, Callback, UserAcc2, Db, StartSeq,
                             <<"">>, Timeout, TimeoutFun),
            try
                keep_sending_changes(
                    Args#changes_args{dir=fwd},
                    Acc0,
                    true)
            after
                couch_db_update_notifier:stop(Notify),
                get_rest_db_updated(ok) % clean out any remaining update messages
            end
        end;
    false ->
        fun(CallbackAcc) ->
            {Callback, UserAcc} = get_callback_acc(CallbackAcc),
            UserAcc2 = start_sending_changes(Callback, UserAcc, Feed),
            {Timeout, TimeoutFun} = get_changes_timeout(Args, Callback),
            {Db, StartSeq} = Start(),
            Acc0 = build_acc(Args#changes_args{feed="normal"}, Callback,
                             UserAcc2, Db, StartSeq, <<>>, Timeout, TimeoutFun),
            {ok, #changes_acc{seq = LastSeq, user_acc = UserAcc3}} =
                send_changes(
                    Args#changes_args{feed="normal"},
                    Acc0,
                    true),
            end_sending_changes(Callback, UserAcc3, LastSeq, Feed)
        end
    end.

get_callback_acc({Callback, _UserAcc} = Pair) when is_function(Callback, 3) ->
    Pair;
get_callback_acc(Callback) when is_function(Callback, 2) ->
    {fun(Ev, Data, _) -> Callback(Ev, Data) end, ok}.


configure_filter("_doc_ids", Style, Req, _Db) ->
    {doc_ids, Style, get_doc_ids(Req)};
configure_filter("_design", Style, _Req, _Db) ->
    {design_docs, Style};
configure_filter("_view", Style, Req, Db) ->
    ViewName = couch_httpd:qs_value(Req, "view", ""),
    if ViewName /= "" -> ok; true ->
        throw({bad_request, "`view` filter parameter is not provided."})
    end,
    ViewNameParts = string:tokens(ViewName, "/"),
    case [?l2b(couch_httpd:unquote(Part)) || Part <- ViewNameParts] of
        [DName, VName] ->
            {ok, DDoc} = open_ddoc(Db, <<"_design/", DName/binary>>),
            check_member_exists(DDoc, [<<"views">>, VName]),
            {view, Style, DDoc, VName};
        [] ->
            Msg = "`view` must be of the form `designname/viewname`",
            throw({bad_request, Msg})
    end;
configure_filter([$_ | _], _Style, _Req, _Db) ->
    throw({bad_request, "unknown builtin filter name"});
configure_filter("", main_only, _Req, _Db) ->
    {default, main_only};
configure_filter("", all_docs, _Req, _Db) ->
    {default, all_docs};
configure_filter(FilterName, Style, Req, Db) ->
    FilterNameParts = string:tokens(FilterName, "/"),
    case [?l2b(couch_httpd:unquote(Part)) || Part <- FilterNameParts] of
        [DName, FName] ->
            {ok, DDoc} = open_ddoc(Db, <<"_design/", DName/binary>>),
            check_member_exists(DDoc, [<<"filters">>, FName]),
            {custom, Style, Req, DDoc, FName};
        [] ->
            {default, Style};
        _Else ->
            Msg = "`filter` must be of the form `designname/filtername`",
            throw({bad_request, Msg})
    end.


filter(Db, #full_doc_info{}=FDI, Filter) ->
    filter(Db, couch_doc:to_doc_info(FDI), Filter);
filter(_Db, DocInfo, {default, Style}) ->
    apply_style(DocInfo, Style);
filter(_Db, DocInfo, {doc_ids, Style, DocIds}) ->
    case lists:member(DocInfo#doc_info.id, DocIds) of
        true ->
            apply_style(DocInfo, Style);
        false ->
            []
    end;
filter(_Db, DocInfo, {design_docs, Style}) ->
    case DocInfo#doc_info.id of
        <<"_design", _/binary>> ->
            apply_style(DocInfo, Style);
        _ ->
            []
    end;
filter(Db, DocInfo, {view, Style, DDoc, VName}) ->
    Docs = open_revs(Db, DocInfo, Style),
    {ok, Passes} = couch_query_servers:filter_view(DDoc, VName, Docs),
    filter_revs(Passes, Docs);
filter(Db, DocInfo, {custom, Style, Req0, DDoc, FName}) ->
    Req = case Req0 of
        {json_req, _} -> Req0;
        #httpd{} -> {json_req, couch_httpd_external:json_req_obj(Req0, Db)}
    end,
    Docs = open_revs(Db, DocInfo, Style),
    {ok, Passes} = couch_query_servers:filter_docs(Req, Db, DDoc, FName, Docs),
    filter_revs(Passes, Docs).


get_doc_ids({json_req, {Props}}) ->
    check_docids(couch_util:get_value(<<"doc_ids">>, Props));
get_doc_ids(#httpd{method='POST'}=Req) ->
    {Props} = couch_httpd:json_body_obj(Req),
    check_docids(couch_util:get_value(<<"doc_ids">>, Props));
get_doc_ids(#httpd{method='GET'}=Req) ->
    DocIds = ?JSON_DECODE(couch_httpd:qs_value(Req, "doc_ids", "null")),
    check_docids(DocIds);
get_doc_ids(_) ->
    throw({bad_request, no_doc_ids_provided}).


check_docids(DocIds) when is_list(DocIds) ->
    lists:foreach(fun
        (DocId) when not is_binary(DocId) ->
            Msg = "`doc_ids` filter parameter is not a list of binaries.",
            throw({bad_request, Msg});
        (_) -> ok
    end, DocIds),
    DocIds;
check_docids(_) ->
    Msg = "`doc_ids` filter parameter is not a list of binaries.",
    throw({bad_request, Msg}).


open_ddoc(#db{name= <<"shards/", _/binary>> =ShardName}, DDocId) ->
    {_, Ref} = spawn_monitor(fun() ->
        exit(fabric:open_doc(mem3:dbname(ShardName), DDocId, []))
    end),
    receive
        {'DOWN', Ref, _, _, {ok, _}=Response} ->
            Response;
        {'DOWN', Ref, _, _, Response} ->
            throw(Response)
    end;
open_ddoc(Db, DDocId) ->
    case couch_db:open_doc(Db, DDocId, [ejson_body]) of
        {ok, _} = Resp -> Resp;
        Else -> throw(Else)
    end.


check_member_exists(#doc{body={Props}}, Path) ->
    couch_util:get_nested_json_value({Props}, Path).


apply_style(#doc_info{revs=Revs}, main_only) ->
    [#rev_info{rev=Rev} | _] = Revs,
    [{[{<<"rev">>, couch_doc:rev_to_str(Rev)}]}];
apply_style(#doc_info{revs=Revs}, all_docs) ->
    [{[{<<"rev">>, couch_doc:rev_to_str(R)}]} || #rev_info{rev=R} <- Revs].


open_revs(Db, DocInfo, Style) ->
    DocInfos = case Style of
        main_only -> [DocInfo];
        all_docs -> [DocInfo#doc_info{revs=[R]}|| R <- DocInfo#doc_info.revs]
    end,
    OpenOpts = [deleted, conflicts],
    % Relying on list comprehensions to silence errors
    OpenResults = [couch_db:open_doc(Db, DI, OpenOpts) || DI <- DocInfos],
    [Doc || {ok, Doc} <- OpenResults].


filter_revs(Passes, Docs) ->
    lists:flatmap(fun
        ({true, #doc{revs={RevPos, [RevId | _]}}}) ->
            RevStr = couch_doc:rev_to_str({RevPos, RevId}),
            Change = {[{<<"rev">>, RevStr}]},
            [Change];
        (_) ->
            []
    end, lists:zip(Passes, Docs)).


get_changes_timeout(Args, Callback) ->
    #changes_args{
        heartbeat = Heartbeat,
        timeout = Timeout,
        feed = ResponseType
    } = Args,
    DefaultTimeout = list_to_integer(
        config:get("httpd", "changes_timeout", "60000")
    ),
    case Heartbeat of
    undefined ->
        case Timeout of
        undefined ->
            {DefaultTimeout, fun(UserAcc) -> {stop, UserAcc} end};
        infinity ->
            {infinity, fun(UserAcc) -> {stop, UserAcc} end};
        _ ->
            {lists:min([DefaultTimeout, Timeout]),
                fun(UserAcc) -> {stop, UserAcc} end}
        end;
    true ->
        {DefaultTimeout,
            fun(UserAcc) -> {ok, Callback(timeout, ResponseType, UserAcc)} end};
    _ ->
        {lists:min([DefaultTimeout, Heartbeat]),
            fun(UserAcc) -> {ok, Callback(timeout, ResponseType, UserAcc)} end}
    end.

start_sending_changes(_Callback, UserAcc, ResponseType)
        when ResponseType =:= "continuous"
        orelse ResponseType =:= "eventsource" ->
    UserAcc;
start_sending_changes(Callback, UserAcc, ResponseType) ->
    Callback(start, ResponseType, UserAcc).

build_acc(Args, Callback, UserAcc, Db, StartSeq, Prepend, Timeout, TimeoutFun) ->
    #changes_args{
        include_docs = IncludeDocs,
        conflicts = Conflicts,
        limit = Limit,
        feed = ResponseType,
        filter_fun = Filter
    } = Args,
    #changes_acc{
        db = Db,
        seq = StartSeq,
        prepend = Prepend,
        filter = Filter,
        callback = Callback,
        user_acc = UserAcc,
        resp_type = ResponseType,
        limit = Limit,
        include_docs = IncludeDocs,
        conflicts = Conflicts,
        timeout = Timeout,
        timeout_fun = TimeoutFun
    }.

send_changes(Args, Acc0, FirstRound) ->
    #changes_args{
        dir = Dir
    } = Args,
    #changes_acc{
        db = Db,
        seq = StartSeq,
        filter = Filter
    } = Acc0,
    EnumFun = fun ?MODULE:changes_enumerator/2,
    case can_optimize(FirstRound, Filter) of
        {true, Fun} ->
            Fun(Db, StartSeq, Dir, EnumFun, Acc0, Filter);
        _ ->
            couch_db:changes_since(Db, StartSeq, EnumFun, [{dir, Dir}], Acc0)
    end.


can_optimize(true, {doc_ids, _Style, DocIds})
        when length(DocIds) =< ?MAX_DOC_IDS ->
    {true, fun send_changes_doc_ids/6};
can_optimize(true, {design_docs, _Style}) ->
    {true, fun send_changes_design_docs/6};
can_optimize(_, _) ->
    false.


send_changes_doc_ids(Db, StartSeq, Dir, Fun, Acc0, {doc_ids, _Style, DocIds}) ->
    Lookups = couch_btree:lookup(Db#db.id_tree, DocIds),
    FullInfos = lists:foldl(fun
        ({ok, FDI}, Acc) -> [FDI | Acc];
        (not_found, Acc) -> Acc
    end, [], Lookups),
    send_lookup_changes(FullInfos, StartSeq, Dir, Db, Fun, Acc0).


send_changes_design_docs(Db, StartSeq, Dir, Fun, Acc0, {design_docs, _Style}) ->
    FoldFun = fun(FullDocInfo, _, Acc) ->
        {ok, [FullDocInfo | Acc]}
    end,
    KeyOpts = [{start_key, <<"_design/">>}, {end_key_gt, <<"_design0">>}],
    {ok, _, FullInfos} = couch_btree:fold(Db#db.id_tree, FoldFun, [], KeyOpts),
    send_lookup_changes(FullInfos, StartSeq, Dir, Db, Fun, Acc0).


send_lookup_changes(FullDocInfos, StartSeq, Dir, Db, Fun, Acc0) ->
    FoldFun = case Dir of
        fwd -> fun lists:foldl/3;
        rev -> fun lists:foldr/3
    end,
    GreaterFun = case Dir of
        fwd -> fun(A, B) -> A > B end;
        rev -> fun(A, B) -> A =< B end
    end,
    DocInfos = lists:foldl(fun(FDI, Acc) ->
        DI = couch_doc:to_doc_info(FDI),
        case GreaterFun(DI#doc_info.high_seq, StartSeq) of
            true -> [DI | Acc];
            false -> Acc
        end
    end, [], FullDocInfos),
    SortedDocInfos = lists:keysort(#doc_info.high_seq, DocInfos),
    FinalAcc = try
        FoldFun(fun(DocInfo, Acc) ->
            case Fun(DocInfo, Acc) of
                {ok, NewAcc} ->
                    NewAcc;
                {stop, NewAcc} ->
                    throw({stop, NewAcc})
            end
        end, Acc0, SortedDocInfos)
    catch
        {stop, Acc} -> Acc
    end,
    case Dir of
        fwd -> {ok, FinalAcc#changes_acc{seq = couch_db:get_update_seq(Db)}};
        rev -> {ok, FinalAcc}
    end.


keep_sending_changes(Args, Acc0, FirstRound) ->
    #changes_args{
        feed = ResponseType,
        limit = Limit,
        db_open_options = DbOptions
    } = Args,

    {ok, ChangesAcc} = send_changes(
        Args#changes_args{dir=fwd},
        Acc0,
        FirstRound),
    #changes_acc{
        db = Db, callback = Callback, timeout = Timeout, timeout_fun = TimeoutFun,
        seq = EndSeq, prepend = Prepend2, user_acc = UserAcc2, limit = NewLimit
    } = ChangesAcc,

    couch_db:close(Db),
    if Limit > NewLimit, ResponseType == "longpoll" ->
        end_sending_changes(Callback, UserAcc2, EndSeq, ResponseType);
    true ->
        case wait_db_updated(Timeout, TimeoutFun, UserAcc2) of
        {updated, UserAcc4} ->
            DbOptions1 = [{user_ctx, Db#db.user_ctx} | DbOptions],
            case couch_db:open(Db#db.name, DbOptions1) of
            {ok, Db2} ->
                keep_sending_changes(
                  Args#changes_args{limit=NewLimit},
                  ChangesAcc#changes_acc{
                    db = Db2,
                    user_acc = UserAcc4,
                    seq = EndSeq,
                    prepend = Prepend2,
                    timeout = Timeout,
                    timeout_fun = TimeoutFun},
                  false);
            _Else ->
                end_sending_changes(Callback, UserAcc2, EndSeq, ResponseType)
            end;
        {stop, UserAcc4} ->
            end_sending_changes(Callback, UserAcc4, EndSeq, ResponseType)
        end
    end.

end_sending_changes(Callback, UserAcc, EndSeq, ResponseType) ->
    Callback({stop, EndSeq}, ResponseType, UserAcc).

changes_enumerator(DocInfo, #changes_acc{resp_type = ResponseType} = Acc)
        when ResponseType =:= "continuous"
        orelse ResponseType =:= "eventsource" ->
    #changes_acc{
        filter = Filter, callback = Callback,
        user_acc = UserAcc, limit = Limit, db = Db,
        timeout = Timeout, timeout_fun = TimeoutFun
    } = Acc,
    #doc_info{high_seq = Seq} = DocInfo,
    Results0 = filter(Db, DocInfo, Filter),
    Results = [Result || Result <- Results0, Result /= null],
    %% TODO: I'm thinking this should be < 1 and not =< 1
    Go = if Limit =< 1 -> stop; true -> ok end,
    case Results of
    [] ->
        {Done, UserAcc2} = maybe_heartbeat(Timeout, TimeoutFun, UserAcc),
        case Done of
        stop ->
            {stop, Acc#changes_acc{seq = Seq, user_acc = UserAcc2}};
        ok ->
            {Go, Acc#changes_acc{seq = Seq, user_acc = UserAcc2}}
        end;
    _ ->
        ChangesRow = changes_row(Results, DocInfo, Acc),
        UserAcc2 = Callback({change, ChangesRow, <<>>}, ResponseType, UserAcc),
        reset_heartbeat(),
        {Go, Acc#changes_acc{seq = Seq, user_acc = UserAcc2, limit = Limit - 1}}
    end;
changes_enumerator(DocInfo, Acc) ->
    #changes_acc{
        filter = Filter, callback = Callback, prepend = Prepend,
        user_acc = UserAcc, limit = Limit, resp_type = ResponseType, db = Db,
        timeout = Timeout, timeout_fun = TimeoutFun
    } = Acc,
    #doc_info{high_seq = Seq} = DocInfo,
    Results0 = filter(Db, DocInfo, Filter),
    Results = [Result || Result <- Results0, Result /= null],
    Go = if (Limit =< 1) andalso Results =/= [] -> stop; true -> ok end,
    case Results of
    [] ->
        {Done, UserAcc2} = maybe_heartbeat(Timeout, TimeoutFun, UserAcc),
        case Done of
        stop ->
            {stop, Acc#changes_acc{seq = Seq, user_acc = UserAcc2}};
        ok ->
            {Go, Acc#changes_acc{seq = Seq, user_acc = UserAcc2}}
        end;
    _ ->
        ChangesRow = changes_row(Results, DocInfo, Acc),
        UserAcc2 = Callback({change, ChangesRow, Prepend}, ResponseType, UserAcc),
        reset_heartbeat(),
        {Go, Acc#changes_acc{
            seq = Seq, prepend = <<",\n">>,
            user_acc = UserAcc2, limit = Limit - 1}}
    end.


changes_row(Results, DocInfo, Acc) ->
    #doc_info{
        id = Id, high_seq = Seq, revs = [#rev_info{deleted = Del} | _]
    } = DocInfo,
    #changes_acc{db = Db, include_docs = IncDoc, conflicts = Conflicts} = Acc,
    {[{<<"seq">>, Seq}, {<<"id">>, Id}, {<<"changes">>, Results}] ++
        deleted_item(Del) ++ case IncDoc of
            true ->
                Opts = case Conflicts of
                    true -> [deleted, conflicts];
                    false -> [deleted]
                end,
                Doc = couch_index_util:load_doc(Db, DocInfo, Opts),
                case Doc of
                    null -> [{doc, null}];
                    _ ->  [{doc, couch_doc:to_json_obj(Doc, [])}]
                end;
            false ->
                []
        end}.

deleted_item(true) -> [{<<"deleted">>, true}];
deleted_item(_) -> [].

% waits for a db_updated msg, if there are multiple msgs, collects them.
wait_db_updated(Timeout, TimeoutFun, UserAcc) ->
    receive
    db_updated ->
        get_rest_db_updated(UserAcc)
    after Timeout ->
        {Go, UserAcc2} = TimeoutFun(UserAcc),
        case Go of
        ok ->
            wait_db_updated(Timeout, TimeoutFun, UserAcc2);
        stop ->
            {stop, UserAcc2}
        end
    end.

get_rest_db_updated(UserAcc) ->
    receive
    db_updated ->
        get_rest_db_updated(UserAcc)
    after 0 ->
        {updated, UserAcc}
    end.

reset_heartbeat() ->
    case get(last_changes_heartbeat) of
    undefined ->
        ok;
    _ ->
        put(last_changes_heartbeat, now())
    end.

maybe_heartbeat(Timeout, TimeoutFun, Acc) ->
    Before = get(last_changes_heartbeat),
    case Before of
    undefined ->
        {ok, Acc};
    _ ->
        Now = now(),
        case timer:now_diff(Now, Before) div 1000 >= Timeout of
        true ->
            Acc2 = TimeoutFun(Acc),
            put(last_changes_heartbeat, Now),
            Acc2;
        false ->
            {ok, Acc}
        end
    end.
