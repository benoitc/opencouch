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

-module(couch_app).

-behaviour(application).

-export([start/2, stop/1]).
-export([get_env/1, get_env/2]).

start(_Type, _) ->
    couch_sup:start_link().

stop(_) ->
    ok.

get_env(Key) ->
    get_env(Key, undefined).

get_env(Key, Default) ->
    case application:get_env(couch, Key) of
        undefined -> Default;
        {ok, Val} -> Val
    end.
