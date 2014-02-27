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

%
% This causes an OS process to spawned and it is notified every time a database
% is updated.
%
% The notifications are in the form of a the database name sent as a line of
% text to the OS processes stdout.
%

-module(couch_event).

-behaviour(gen_event).

-export([start_link/1, notify/1]).
-export([init/1, terminate/2, handle_event/2, handle_call/2,
         handle_info/2, code_change/3,stop/1]).

start_link(Consumer) ->
    HandlerId = {?MODULE, make_ref()},
    couch_event_sup:start_link(couch_db_update, HandlerId, Consumer).

notify(Event) ->
    gen_event:notify(couch_db_update, Event).

stop(Pid) ->
    couch_event_sup:stop(Pid).


init(Consumer) ->
    process_flag(trap_exit, true),
    {ok, Consumer}.

handle_event(Event, Consumer) ->
    dispatch_event(Event, Consumer).

handle_call(_Req, Consumer) ->
    {reply, ok, Consumer}.

handle_info({'EXIT', _, _}, _Consumer) ->
    remove_handler;
handle_info(_Info, Consumer) ->
    {ok, Consumer}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _Consumer) ->
    ok.


dispatch_event(Event, Fun) when is_function(Fun) ->
    Fun(Event),
    {ok, Fun};
dispatch_event(Event, {Fun, Acc}) when is_function(Fun) ->
    Acc2 = Fun(Event, Acc),
    {ok, {Fun, Acc2}};
dispatch_event(Event, Pid) when is_pid(Pid) ->
    Pid ! Event,
    {ok, Pid}.
