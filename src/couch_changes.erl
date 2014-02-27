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

-export([handle_changes/3, handle_changes/4]).

% For the builtin filter _docs_ids, this is the maximum number
% of documents for which we trigger the optimized code path.
-define(MAX_DOC_IDS, 100).

-define(DEFAULT_LIMIT, 1000000000000000).

-record(state, {dbname,
                db_options=[],
                since,
                callback,
                acc,
                user_timeout,
                timeout,
                heartbeat,
                timeout_acc=0,
                notifier,
                stream,
                filter=nil,
                style=main_only,
                include_docs=false,
                dir=fwd,
                limit,
                conflicts}).

-type changes_stream() :: true | false | once.
-type changes_filter() :: nil | {doc_ids, list()} | {prefix, binary()} | function().
-type changes_options() :: [{stream, changes_stream()} |
                            {since, integer()} |
                            {filter, changes_filter()} |
                            {timeout, integer()} |
                            {heartbeat, true | integer()} |
                            {style, main_only | all_docs} |
                            {include_docs, true | false} |
                            {db_options, list()} |
                            {dir, fwd | rev} |
                            {limit, integer()} |
                            conflicts | {conflicts, true | false}].

-export_type([changes_filter/0]).
-export_type([changes_stream/0]).
-export_type([changes_options/0]).


-spec handle_changes(binary(), function(), term()) -> ok | {error, term()}.
handle_changes(DbName, Fun, Acc) ->
    handle_changes(DbName, Fun, Acc, []).

%% @doc function returning changes in a streaming fashion if needed.
-spec handle_changes(binary(), function(), term(),
                     changes_options()) -> ok | {error, term()}.
handle_changes(DbName, Fun, Acc, Options) ->
    Since = proplists:get_value(since, Options, 0),
    Stream = proplists:get_value(stream, Options, false),
    Filter = proplists:get_value(filter, Options, nil),
    Style = proplists:get_value(filter, Options, main_only),
    IncludeDocs = proplists:get_value(include_docs, Options, false),
    DbOptions = proplists:get_value(db_options, Options, []),
    Dir = proplists:get_value(dir, Options, fwd),
    Limit =  proplists:get_value(limit, Options, ?DEFAULT_LIMIT),
    Conflicts = proplists:get_value(conflicts, Options, false),

    State0 = #state{dbname=DbName,
                    db_options=DbOptions,
                    since=Since,
                    callback=Fun,
                    acc=Acc,
                    filter=Filter,
                    style=Style,
                    include_docs=IncludeDocs,
                    dir=Dir,
                    limit=Limit,
                    conflicts=Conflicts},

    case send_changes(State0) of
        {ok, #state{since=LastSeq, acc=Acc2}=State} ->
            case Stream of
                true ->
                    start_loop(State#state{stream=true}, Options);
                once when LastSeq =:= Since ->
                    start_loop(State#state{stream=once}, Options);
                _ ->
                    Fun(stop, {LastSeq, Acc2})
            end;
        {stop, #state{since=LastSeq, acc=Acc2}} ->
                Fun(stop, {LastSeq, Acc2});
        Error ->
            Error
    end.


start_loop(#state{dbname=DbName}=State, Options) ->
    {UserTimeout, Timeout, Heartbeat} = changes_timeout(Options),
    Notifier = db_update_notifier(DbName),
    try
        loop(State#state{notifier=Notifier,
                         user_timeout=UserTimeout,
                         timeout=Timeout,
                         heartbeat=Heartbeat})
    after
        couch_event:stop(Notifier)
    end.

loop(#state{since=Since, callback=Callback, acc=Acc,
            user_timeout=UserTimeout, timeout=Timeout,
            heartbeat=Heartbeat, timeout_acc=TimeoutAcc,
            stream=Stream}=State) ->
    receive
        db_updated ->
            case send_changes(State) of
                {ok, State2} when Stream =:= true ->
                    loop(State2#state{timeout_acc=0});
                {ok, #state{since=LastSeq, acc=Acc2}} ->
                    Callback(stop, {LastSeq, Acc2});
                {stop, #state{since=LastSeq, acc=Acc2}} ->
                    Callback(stop, {LastSeq, Acc2})
            end;
        db_deleted ->
            Callback(stop, {Since, Acc})
    after Timeout ->
            TimeoutAcc2 = TimeoutAcc + Timeout,
            case UserTimeout =< TimeoutAcc2 of
                true ->
                    Callback(stop, {Since, Acc});
                false when Heartbeat =:= true ->
                    case Callback(heartbeat, Acc) of
                        {ok, Acc2} ->
                            loop(State#state{acc=Acc2,
                                             timeout_acc=TimeoutAcc2});
                        {stop, Acc2} ->
                            Callback(stop, {Since, Acc2})
                    end;
                _ ->
                    Callback(stop, {Since, Acc})
            end
    end.

changes_timeout(Options) ->
    DefaultTimeout = couch_app:get_env(changes_timeout,  60000),
    UserTimeout = proplists:get_value(timeout, Options, DefaultTimeout),
    {Timeout, Heartbeat} = case proplists:get_value(heartbeat, Options) of
        undefined -> {UserTimeout, false};
        true ->
            T = erlang:min(DefaultTimeout, UserTimeout),
            {T, true};
        H ->
            T = erlang:min(H, UserTimeout),
            {T, true}
    end,
    {UserTimeout, Timeout, Heartbeat}.


send_changes(#state{dbname=DbName, db_options=DbOptions,
                    filter=Filter}=State) ->

    case couch_db:open(DbName, DbOptions) of
        {ok, Db} ->
            try
                case Filter of
                    {doc_ids, DocIds} when length(DocIds) =< ?MAX_DOC_IDS ->
                        send_changes_doc_ids(Db, DocIds, State);
                    {prefix, Prefix} ->
                        send_changes_prefix(Db, Prefix, State);
                    _ ->
                        send_changes_since(Db, State)
                end
            after
                catch couch_db:close(Db)
            end;
        _Error ->
            {stop, State}
        end.


send_changes_since(Db, #state{since=StartSeq, dir=Dir}=State) ->
    EnumFun = fun(DocInfo, {_Go, Db1, State1}) ->
            {Go, NState} = change_enumerator(DocInfo, Db1, State1),
            {Go, {Go, Db, NState}}
    end,

    Acc0 = {ok, Db, State},
    case couch_db:changes_since(Db, StartSeq, EnumFun,  [{dir, Dir}],
                                Acc0) of

        {ok, {Go2, _, State2}} ->
            {Go2, State2};
        Error ->
            lager:error("Error while getting changes: ~p~n", [Error]),
            {stop, State}
    end.

send_changes_doc_ids(Db, DocIds, State) ->
    Lookups = couch_btree:lookup(Db#db.id_tree, DocIds),
    FullInfos = lists:foldl(fun
                ({ok, FDI}, Acc) -> [FDI | Acc];
                (not_found, Acc) -> Acc
            end, [], Lookups),
    send_lookup_changes(FullInfos, Db, State).

send_changes_prefix(Db, Prefix, State) ->
    FoldFun = fun(FullDocInfo, _, Acc) ->
            {ok, [FullDocInfo | Acc]}
    end,
    KeyOpts = [{start_key, Prefix}, {end_key_gt, <<Prefix/binary, "0">>}],
    {ok, _, FullInfos} = couch_btree:fold(Db#db.id_tree, FoldFun, [], KeyOpts),
    send_lookup_changes(FullInfos, Db, State).


send_lookup_changes(FullDocInfos, Db, #state{since=StartSeq,
                                             dir=Dir}=State) ->
    %% function to test if the doc sequence is > or < to StartSeq
    GreaterFun = case Dir of
        fwd -> fun(A, B) -> A > B end;
        rev -> fun(A, B) -> A =< B end
    end,

    %% get docs greater than StartSeq and orderd by seq.
    DocInfos = lists:foldl(fun(FDI, Acc) ->
        DI = couch_doc:to_doc_info(FDI),
        case GreaterFun(DI#doc_info.high_seq, StartSeq) of
            true -> [DI | Acc];
            false -> Acc
        end
    end, [], FullDocInfos),
    SortedDocInfos = lists:keysort(#doc_info.high_seq, DocInfos),

    SortedDocInfos2 = case Dir of
        fwd -> SortedDocInfos;
        rev -> lists:reverse(SortedDocInfos)
    end,

    {Go, FinalState} = fold(SortedDocInfos2, {ok, Db, State}),
    case Dir of
        fwd -> {Go, FinalState#state{since=couch_db:get_update_seq(Db)}};
        rev -> {Go, FinalState}
    end.

fold([], {Go, _Db, State}) ->
    {Go, State};
fold([DocInfo | Rest], {_Go, Db, State}) ->
    case change_enumerator(DocInfo, Db, State) of
        {ok, State2} ->
            fold(Rest, {ok, Db, State2});
        {stop, State2} ->
            {stop, State2}
    end.

change_enumerator(DocInfo, Db, State) ->
    #state{callback = Callback,
           acc = Acc,
           stream = Stream,
           style = Style,
           limit = Limit,
           filter = Filter} = State,

    #doc_info{high_seq = Seq} = DocInfo,
    Results0 = filter(Db, DocInfo, Filter, Style),
    Results = [Result || Result <- Results0, Result /= null],

    Go = if (Limit =< 1) andalso Results =/= [] ->
            stop;
        true ->
            ok
    end,
    case Results of
        [] when Stream /= once, Stream /= true ->
            {stop, State#state{since=Seq}};
        [] ->
            {ok, State#state{since=Seq}};
        _ ->
            ChangesRow = changes_row(Results, DocInfo, Db, State),
            {Go, Acc2} = Callback(ChangesRow, Acc),
            {Go, State#state{since=Seq, acc=Acc2}}
    end.


changes_row(Results, DocInfo, Db, #state{include_docs=IncDoc,
                                         conflicts=Conflicts}) ->
    #doc_info{id = Id,
              high_seq = Seq,
              revs = [#rev_info{deleted = Del} | _]} = DocInfo,

    {[{<<"seq">>, Seq}, {<<"id">>, Id}, {<<"changes">>, Results}] ++
     deleted_item(Del) ++ case IncDoc of
            true ->
                Opts = case Conflicts of
                    true -> [deleted, conflicts];
                    false -> [deleted]
                end,
                Doc = couch_util:load_doc(Db, DocInfo, Opts),
                case Doc of
                    null -> [{doc, null}];
                    _ ->  [{doc, couch_doc:to_json_obj(Doc, [])}]
                end;
            false ->
                []
        end}.

deleted_item(true) -> [{<<"deleted">>, true}];
deleted_item(_) -> [].


%% filter the doc and apply the style
filter(Db, #full_doc_info{}=FDI, Filter, Style) ->
    filter(Db, couch_doc:to_doc_info(FDI), Filter, Style);

filter(_Db, DocInfo, {doc_ids, DocIds}, Style) ->
    case lists:member(DocInfo#doc_info.id, DocIds) of
        true ->
            apply_style(DocInfo, Style);
        false ->
            []
    end;
filter(_Db, DocInfo, {prefix, Prefix}, Style) ->
    Len = size(Prefix),
    case DocInfo#doc_info.id of
        << Prefix:Len/binary, _/binary>> ->
            apply_style(DocInfo, Style);
        _ ->
            []
    end;
filter(Db, DocInfo, Fun, Style) when is_function(Fun) ->
    Docs = open_revs(Db, DocInfo, Style),
    JsonDocs = [couch_doc:to_json_obj(Doc, [revs]) || Doc <- Docs],

    FilterWrapperFun = fun(Doc) ->
            case Fun(Doc) of
                true -> true;
                false -> false;
                Error ->
                    lager:error("filter error: ~p~n", [Error]),
                    false
            end
    end,
    Passes = lists:map(FilterWrapperFun, JsonDocs),
    filter_revs(Passes, Docs);
filter(_Db, DocInfo, _, Style) ->
    apply_style(DocInfo, Style).


open_revs(Db, DocInfo, Style) ->
    DocInfos = case Style of
        main_only -> [DocInfo];
        all_docs -> [DocInfo#doc_info{revs=[R]}|| R <- DocInfo#doc_info.revs]
    end,
    OpenOpts = [deleted, conflicts],
    % Relying on list comprehensions to silence errors
    OpenResults = [couch_db:open_doc(Db, DI, OpenOpts) || DI <- DocInfos],
    [Doc || {ok, Doc} <- OpenResults].

%% return changed revision when using a filtered function
filter_revs(Passes, Docs) ->
    lists:flatmap(fun
            ({true, #doc{revs={RevPos, [RevId | _]}}}) ->
                RevStr = couch_doc:rev_to_str({RevPos, RevId}),
                Change = {[{<<"rev">>, RevStr}]},
                [Change];
            (_) ->
                []
        end, lists:zip(Passes, Docs)).


apply_style(#doc_info{revs=Revs}, main_only) ->
    [#rev_info{rev=Rev} | _] = Revs,
    [{[{<<"rev">>, couch_doc:rev_to_str(Rev)}]}];
apply_style(#doc_info{revs=Revs}, all_docs) ->
    [{[{<<"rev">>, couch_doc:rev_to_str(R)}]} || #rev_info{rev=R} <- Revs].

db_update_notifier(#db{name=DbName}) ->
    db_update_notifier(DbName);
db_update_notifier(DbName) ->
    Self = self(),
    {ok, NotifierPid} = couch_event:start_link(fun
                ({deleted, Name}) when Name =:= DbName ->
                    Self ! db_deleted;
                ({updated, Name}) when Name =:= DbName ->
                    Self ! db_updated;
                (_) ->
                    ok
            end),
    NotifierPid.
