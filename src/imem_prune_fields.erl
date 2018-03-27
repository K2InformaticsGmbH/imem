%% -----------------------------------------------------------------------------
%%
%% imem_prune_fields.erl: SQL - matching the identifiers
%%                              of a SQL statement.
%%
%% Copyright (c) 2012-18 K2 Informatics GmbH.  All Rights Reserved.
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
%% -----------------------------------------------------------------------------

-module(imem_prune_fields).

-export([
    finalize/2,
    fold/5,
    init/1,
    match/2
]).

match(ParseTree, InFields) ->
    sqlparse_fold:top_down(imem_prune_fields, ParseTree, InFields).

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Setting up parameters.
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec init(InFields :: [binary()]) -> InFields :: [binary()].
init(InFields) when is_list(InFields) ->
    InFields.

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Postprocessing of the result.
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec finalize(InFields :: [binary()], [binary()]|tuple()) -> [binary()]|tuple().
finalize(_InFields, CtxIn) when is_list(CtxIn) ->
    lists:usort(CtxIn).

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Layout methods for processing the various parser subtrees
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec fold(InFields :: [binary()], FunState :: tuple(), Ctx :: [binary()],
    PTree :: list()|tuple(), FoldState :: tuple()) -> Ctx :: list().

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% binary
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(InFields, _FunState, Ctx, PTree, FoldState)
    when is_binary(PTree), element(2, FoldState) == start ->
    add_if(InFields, PTree, Ctx);

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% {atom, binary}
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(InFields, _FunState, Ctx, {Elem1, Elem2} = _PTree, FoldState)
    when is_atom(Elem1), Elem1 =/= extra, is_binary(Elem2),
    element(2, FoldState) == start ->
    add_if(InFields, Elem2, Ctx);

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% {atom, binary, binary}
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(InFields, _FunState, Ctx, {Elem1, Elem2, Elem3} = _PTree, FoldState)
    when is_atom(Elem1), is_binary(Elem2), is_binary(Elem3),
    element(2, FoldState) == start ->
    add_if(InFields, Elem3, add_if(InFields, Elem2, Ctx));

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% {atom, binary, _}
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(InFields, _FunState, Ctx, {Elem1, Elem2, _Elem3} = _PTree, FoldState)
    when is_atom(Elem1), is_binary(Elem2), element(2, FoldState) == start ->
    add_if(InFields, Elem2, Ctx);

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% {atom, _, binary}
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(InFields, _FunState, Ctx, {Elem1, _Elem2, Elem3} = _PTree, FoldState)
    when is_atom(Elem1), is_binary(Elem3), element(2, FoldState) == start ->
    add_if(InFields, Elem3, Ctx);

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% {atom, binary, _, _}
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(InFields, _FunState, Ctx, {Elem1, Elem2, _Elem3, _Elem4} =
    _PTree, FoldState)
    when is_atom(Elem1), is_binary(Elem2), element(2, FoldState) == start ->
    add_if(InFields, Elem2, add_if(InFields, Elem2, Ctx));

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% {binary, binary}
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(InFields, _FunState, Ctx, {Elem1, Elem2} = _PTree, FoldState)
    when is_binary(Elem1), is_binary(Elem2), element(2, FoldState) == start ->
    add_if(InFields, Elem2, add_if(InFields, Elem1, Ctx));

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% NO ACTION.
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

fold(_InFields, _FunState, Ctx, _PTree, _FoldState) ->
    Ctx.

% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Helper functions.
% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

add_if(InFields, Elem, Ctx) ->
    case lists:member(Elem, InFields) of
        true -> [Elem | Ctx];
        _ -> Ctx
    end.
