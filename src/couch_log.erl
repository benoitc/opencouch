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

-module(couch_log).

-compile([{parse_transform, lager_transform}]).


-export([debug/2, info/2, notice/2, warning/2, error/2, critical/2,
         alert/2, emergency/2]).

-export([set_loglevel/1]).

debug(Fmt, Args) ->
    lager:debug(Fmt, Args).

info(Fmt, Args) ->
    lager:info(Fmt, Args).

notice(Fmt, Args) ->
    lager:notice(Fmt, Args).

warning(Fmt, Args) ->
    lager:warning(Fmt, Args).

error(Fmt, Args) ->
    lager:error(Fmt, Args).

critical(Fmt, Args) ->
    lager:critical(Fmt, Args).

alert(Fmt, Args) ->
    lager:alert(Fmt, Args).

emergency(Fmt, Args) ->
    lager:emergency(Fmt, Args).


set_loglevel(Level) ->
    case application:get_env(lager, handlers) of
        undefined -> ok;
        {ok, Handlers} ->
            lists:foreach(fun(Handler) ->
                        lager:set_loglevel(Handler, ALevel)
                end, Handlers)
    end.
