%%% A (simple) diff

%%% Copyright (C) 2011  Tomas Abrahamsson
%%%
%%% Author: Tomas Abrahamsson <tab@lysator.liu.se>
%%%
%%% This library is free software; you can redistribute it and/or
%%% modify it under the terms of the GNU Library General Public
%%% License as published by the Free Software Foundation; either
%%% version 2 of the License, or (at your option) any later version.
%%%
%%% This library is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
%%% Library General Public License for more details.
%%%
%%% You should have received a copy of the GNU Library General Public
%%% License along with this library; if not, write to the Free
%%% Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
%%%

-module(imem_tdiff).
-export([diff/2, diff/3, patch/2]).
-export([diff_orig/2, diff_orig/3]).
-export([diff_files/2, diff_files/3]).
-export([diff_binaries/2, diff_binaries/3, patch_binaries/2]).
-export([format_diff_lines/1]).
-export([print_diff_lines/1]).

-type filename() :: string().
-type options() :: [option()].
-type option() :: {algorithm_tracer, no_tracer | algorithm_tracer()}.
-type algorithm_tracer() :: fun(({d, d()} |
                                 {dpath, dpath()} |
                                 {exhausted_kdiagonals, d()} |
                                 {final_edit_script, edit_script()}) -> _).

-type d() :: integer().  %% The diagonal number, offset in number of steps from
                         %% the diagonal through (0,0).
-type dpath() :: dpath(term()).
-type dpath(Elem) :: {X::index(), Y::index(),
                      SX::[Elem]|oob, SY::[Elem]|oob,
                      [Elem]}.%% The X and Y are indices along x and y.
                              %% The SX and SY are  accumulated old/new strings
                              %% The last is a list of elements in reverse
                              %% order.
-type index() :: non_neg_integer().
-type edit_script() :: edit_script(term()).
-type edit_script(Elem) :: [{eq, [Elem]} | {ins, [Elem]} | {del, [Elem]}].

-export_type([options/0, option/0]).
-export_type([edit_script/0, edit_script/1]).
-export_type([algorithm_tracer/0, d/0, dpath/0, dpath/1, index/0]).

%% @equiv diff_files(F1, F2, [])
-spec diff_files(filename(), filename()) -> edit_script(Line::string()).
diff_files(F1, F2) -> diff_files(F1, F2, _Opts=[]).

%% @doc Read the two files into memory, split to lists of lines
%% and compute the edit-script (or diff) for these.
%% The result is a diff for a list of lines/strings.
-spec diff_files(filename(), filename(), options()) -> edit_script(Line) when
      Line :: string().
diff_files(F1, F2, Opts) ->
    {ok,B1} = file:read_file(F1),
    {ok,B2} = file:read_file(F2),
    diff_binaries(B1, B2, Opts).

%% @equiv diff_binaries(B1, B2, [])
diff_binaries(B1, B2) -> diff_binaries(B1, B2, _Opts=[]).

%% @doc Split the two binaries into lists of lines (lists of strings),
%% and compute the edit-script (or diff) for these.
%% The result is a diff for a list of lines/strings,
%% not for a list of binaries.
-spec diff_binaries(binary(), binary(), options()) -> edit_script(Line) when
      Line :: string().
diff_binaries(B1, B2, Opts) ->
    diff(split_bin_to_lines(B1), split_bin_to_lines(B2), Opts).


split_bin_to_lines(B) -> sbtl(binary_to_list(B), "", []).

sbtl("\n" ++ Rest, L, Acc) -> sbtl(Rest, "", [lists:reverse("\n"++L) | Acc]);
sbtl([C|Rest], L, Acc)     -> sbtl(Rest, [C|L], Acc);
sbtl("", "", Acc)          -> lists:reverse(Acc);
sbtl("", L, Acc)           -> lists:reverse([lists:reverse(L) | Acc]).


%% @doc Print an edit-script, or diff. See {@link format_diff_lines/1}
%% for info on the output.
-spec print_diff_lines(edit_script(char())) -> _.
print_diff_lines(Diff) -> io:format("~s~n", [format_diff_lines(Diff)]).

%% @doc Format an edit-script or diff of lines to text, so that it looks like
%% a diff. The result will look like this if printed:
%% <pre><![CDATA[
%%    123,456
%%    < old line 1
%%    < old line 2
%%    ---
%%    > new line 1
%%    678
%%    > new line 2
%% ]]></pre>
-spec format_diff_lines(edit_script(char())) -> iodata().
format_diff_lines(Diff) -> fdl(Diff, 1,1).

fdl([{del,Ls1},{ins,Ls2}|T], X, Y) ->
    Addr = io_lib:format("~sc~s~n", [fmt_addr(X,Ls1), fmt_addr(Y, Ls2)]),
    Del  = format_lines("< ", Ls1),
    Sep  = io_lib:format("---~n", []),
    Ins  = format_lines("> ", Ls2),
    [Addr, Del, Sep, Ins | fdl(T, X+length(Ls1), Y+length(Ls2))];
fdl([{del,Ls}|T], X, Y) ->
    Addr = io_lib:format("~w,~wd~w~n", [X,X+length(Ls), Y]),
    Del  = format_lines("< ", Ls),
    [Addr, Del | fdl(T, X+length(Ls), Y)];
fdl([{ins,Ls}|T], X, Y) ->
    Addr = io_lib:format("~wa~w,~w~n", [X,Y,Y+length(Ls)]),
    Ins  = format_lines("> ", Ls),
    [Addr, Ins | fdl(T, X, Y+length(Ls))];
fdl([{eq,Ls}|T], X, Y) ->
    fdl(T, X+length(Ls), Y+length(Ls));
fdl([], _X, _Y) ->
    [].

fmt_addr(N, Ls) when length(Ls) == 1 -> f("~w",    [N]);
fmt_addr(N, Ls)                      -> f("~w,~w", [N,N+length(Ls)-1]).

f(F,A) -> lists:flatten(io_lib:format(F,A)).

format_lines(Indicator, Lines) ->
    lists:map(fun(Line) -> io_lib:format("~s~s", [Indicator, Line]) end,
              Lines).

%% Algorithm: "An O(ND) Difference Algorithm and Its Variations"
%% by E. Myers, 1986.
%%
%% Some good info can also be found at http://neil.fraser.name/writing/diff/
%%
%% General principle of the algorithm:
%%
%% We are about to produce a diff (or editscript) on what differs (or
%% how to get from) string Sx to Sy.  We lay out a grid with the
%% symbols from Sx on the x-axis and the symbols from Sy on the Y
%% axis. The first symbol of Sx and Sy is at (0,0).
%%
%% (The Sx and Sy are strings of symbols: lists of lines or lists of
%% characters, or lists of works, or whatever is suitable.)
%%
%% Example: Sx="aXcccXe", Sy="aYcccYe" ==> the following grid is formed:
%%
%%             Sx
%%             aXcccXe
%%         Sy a\
%%            Y
%%            c  \\\
%%            c  \\\
%%            c  \\\
%%            Y
%%            e      \
%%
%% Our plan now is go from corner to corner: from (0,0) to (7,7).
%% We can move diagonally whenever the character on the x-axis and the
%% character on the y-axis are identical. Those are symbolized by the
%% \-edges in the grid above.
%%
%% When it is not possible to go diagonally (because the characters on
%% the x- and y-axis are not identical), we have to go horizontally
%% and vertically. This corresponds to deleting characters from Sx and
%% inserting characters from Sy.
%%
%% Definitions (from the "O(ND) ..." paper by E.Myers):
%%
%% * A D-path is a path with D non-diagonal edges (ie: edges that are
%%   vertical and/or horizontal).
%% * K-diagonal: the diagonal such that K=X-Y
%%   (Thus, the 0-diagonal is the one starting at (0,0), going
%%   straight down-right. The 1-diagonal is the one just to the right of
%%   the 0-diagonal: starting at (1,0) going straight down-right.
%%   There are negative diagonals as well: the -1-diagonal is the one starting
%%   at (0,1), and so on.
%% * Snake: a sequence of only-diagonal steps
%%
%% The algorithm loops over D and over the K-diagonals:
%% D = 0..(length(Sx)+length(Sy))
%%   K = -D..D in steps of 2
%%     For every such K-diagonal, we choose between the (D-1)-paths
%%     whose end-points are currently on the adjacent (K-1)- and
%%     (K+1)-diagonals: we pick the one that have gone furthest along
%%     its diagonal.
%%
%%     This means taking that (D-1)-path and going right (if
%%     we pick the (D-1)-path on the (K-1)-diagonal) or down (if we
%%     pick the (D-1)-path on the (K+1)-diagonal), thus forming a
%%     D-path from a (D-1)-path.
%%
%%     After this, we try to extend the snake as far as possible along
%%     the K-diagonal.
%%
%%     Note that this means that when we choose between the
%%     (D-1)-paths along the (K-1)- and (K+1)-diagonals, we choose
%%     between two paths, whose snakes have been extended as far as
%%     possible, ie: they are at a point where the characters Sx and
%%     Sy don't match.
%%
%% Note that with this algorithm, we always do comparions further
%% right into the strings Sx and Sy. The algorithm never goes towards
%% the beginning of either Sx or Sy do do further comparisons. This is
%% good, because this fits the way lists are built in functional
%% programming languages.

%% @equiv diff(Sx, Sy, [])
-spec diff(Old::[Elem], New::[Elem]) -> edit_script(Elem) when Elem::term().
diff(Sx, Sy) -> diff(Sx, Sy, _Opts=[]).

%% @doc Compute an edit-script  between two sequences of elements,
%% such as two strings, lists of lines, or lists of elements more generally.
%% The result is a list of operations add/del/eq that can transform
%% `Old' to `New'
%%
%% The algorithm is "An O(ND) Difference Algorithm and Its Variations"
%% by E. Myers, 1986.
%%
%% Note: This implementation currently searches only forwards. For
%% large inputs (such as thousands of elements) that differ very much,
%% this implementation will take unnecessarily long time, and may not
%% complete within reasonable time.
%%
%% @end
%% Todo for optimization to handle large inputs (see the paper for details)
%% * Search from both ends as described in the paper.
%%   When passing half of distance, search from the end (reversing
%%   the strings). Stop again at half. If snakes don't meet,
%%   pick the best (or all?) snakes from both ends, search
%%   recursively from both ends within this space.
%% * Keep track of visited coordinates.
%%   If already visited, consider the snake/diagonal dead and don't follow it.


diff_orig(Sx, Sy) -> diff_orig(Sx, Sy, _Opts=[]).

-spec diff(Old::[Elem], New::[Elem], options()) -> edit_script(Elem) when Elem::term().
diff(Sx, Sy, Opts) -> diff_reverse(diff_orig(lists:reverse(Sx), lists:reverse(Sy), Opts)).

diff_reverse(Script) -> diff_reverse(Script, []).

diff_reverse([], Acc) -> Acc;
diff_reverse([{del,EL1},{ins,EL2}|Script], Acc) -> 
    diff_reverse(Script, [{del,lists:reverse(EL1)},{ins,lists:reverse(EL2)}|Acc]);
diff_reverse([{A,EL}|Script], Acc) -> 
    diff_reverse(Script, [{A,lists:reverse(EL)}|Acc]). 

-spec diff_orig(Old::[Elem], New::[Elem], options()) -> edit_script(Elem) when Elem::term().
diff_orig(Sx, Sy, Opts) ->
    SxLen = length(Sx),
    SyLen = length(Sy),
    DMax = SxLen + SyLen,
    Tracer = proplists:get_value(algorithm_tracer, Opts, no_tracer),
    EditScript = case try_dpaths(0, DMax, [{0, 0, Sx, Sy, []}], Tracer) of
                     no            -> [{del,Sx},{ins,Sy}];
                     {ed,EditOpsR} -> edit_ops_to_edit_script(EditOpsR)
                 end,
    t_final_script(Tracer, EditScript),
    EditScript.

try_dpaths(D, DMax, D1Paths, Tracer) when D =< DMax ->
    t_d(Tracer, D),
    case try_kdiagonals(-D, D, D1Paths, [], Tracer) of
        {ed, E}          -> {ed, E};
        {dpaths, DPaths} -> try_dpaths(D+1, DMax, DPaths, Tracer)
    end;
try_dpaths(_, _DMax, _DPaths, _Tracer) ->
    no.

try_kdiagonals(K, D, D1Paths, DPaths, Tracer) when K =< D ->
    DPath = if D == 0 -> hd(D1Paths);
               true   -> pick_best_dpath(K, D, D1Paths)
            end,
    case follow_snake(DPath) of
        {ed, E} ->
            {ed, E};
        {dpath, DPath2} when K =/= -D ->
            t_dpath(Tracer, DPath2),
            try_kdiagonals(K+2, D, tl(D1Paths), [DPath2 | DPaths], Tracer);
        {dpath, DPath2} when K =:= -D ->
            t_dpath(Tracer, DPath2),
            try_kdiagonals(K+2, D, D1Paths, [DPath2 | DPaths], Tracer)
    end;
try_kdiagonals(_, D, _, DPaths, Tracer) ->
    t_exhausted_kdiagonals(Tracer, D),
    {dpaths, lists:reverse(DPaths)}.

follow_snake({X, Y, [H|Tx], [H|Ty], Cs}) -> follow_snake({X+1,Y+1, Tx,Ty,
                                                          [{e,H} | Cs]});
follow_snake({_X,_Y,[],     [],     Cs}) -> {ed, Cs};
follow_snake({X, Y, [],     Sy,     Cs}) -> {dpath, {X, Y, [],  Sy,  Cs}};
follow_snake({X, Y, oob,    Sy,     Cs}) -> {dpath, {X, Y, oob, Sy,  Cs}};
follow_snake({X, Y, Sx,     [],     Cs}) -> {dpath, {X, Y, Sx,  [],  Cs}};
follow_snake({X, Y, Sx,     oob,    Cs}) -> {dpath, {X, Y, Sx,  oob, Cs}};
follow_snake({X, Y, Sx,     Sy,     Cs}) -> {dpath, {X, Y, Sx,  Sy,  Cs}}.

pick_best_dpath(K, D, DPs) -> pbd(K, D, DPs).

pbd( K, D, [DP|_]) when K==-D -> go_inc_y(DP);
pbd( K, D, [DP])   when K==D  -> go_inc_x(DP);
pbd(_K,_D, [DP1,DP2|_])       -> pbd2(DP1,DP2).

pbd2({_,Y1,_,_,_}=DP1, {_,Y2,_,_,_}) when Y1 > Y2 -> go_inc_x(DP1);
pbd2(_DP1  ,           DP2)                       -> go_inc_y(DP2).

go_inc_y({X, Y, [H|Tx], Sy, Cs}) -> {X, Y+1, Tx,  Sy,  [{y,H}|Cs]};
go_inc_y({X, Y, [],     Sy, Cs}) -> {X, Y+1, oob, Sy,  Cs};
go_inc_y({X, Y, oob,    Sy, Cs}) -> {X, Y+1, oob, Sy,  Cs}.

go_inc_x({X, Y, Sx, [H|Ty], Cs}) -> {X+1, Y, Sx,  Ty,  [{x,H}|Cs]};
go_inc_x({X, Y, Sx, [],     Cs}) -> {X+1, Y, Sx,  oob, Cs};
go_inc_x({X, Y, Sx, oob,    Cs}) -> {X+1, Y, Sx,  oob, Cs}.


edit_ops_to_edit_script(EditOps) -> e2e(EditOps, _Acc=[]).

e2e([{x,C}|T], [{ins,R}|Acc]) -> e2e(T, [{ins,[C|R]}|Acc]);
e2e([{y,C}|T], [{del,R}|Acc]) -> e2e(T, [{del,[C|R]}|Acc]);
e2e([{e,C}|T], [{eq,R}|Acc])  -> e2e(T, [{eq, [C|R]}|Acc]);
e2e([{x,C}|T], Acc)           -> e2e(T, [{ins,[C]}|Acc]);
e2e([{y,C}|T], Acc)           -> e2e(T, [{del,[C]}|Acc]);
e2e([{e,C}|T], Acc)           -> e2e(T, [{eq, [C]}|Acc]);
e2e([],        Acc)           -> Acc.

%% @doc Apply a patch, in the form of an edit-script, to a string or
%% list of lines (or list of elements more generally)
-spec patch([Elem], edit_script(Elem)) -> [Elem] when Elem::term().
patch(S, Diff) -> p2(S, Diff, []).

%% @doc Apply a patch to a binary.  The binary is first split to list
%% of lines (list of strings), and the edit-script is expected to be
%% for lists of strings/lines. The result is a list of strings.
-spec patch_binaries(binary(), edit_script(Line)) -> [Line] when
      Line::string().
patch_binaries(B, Diff) ->
    patch(split_bin_to_lines(B), Diff).

p2(S, [{eq,T}|Rest], Acc)  -> p2_eq(S, T, Rest, Acc);
p2(S, [{ins,T}|Rest], Acc) -> p2_ins(S, T, Rest, Acc);
p2(S, [{del,T}|Rest], Acc) -> p2_del(S, T, Rest, Acc);
p2([],[], Acc)             -> lists:reverse(Acc).

p2_eq([H|S], [H|T], Rest, Acc) -> p2_eq(S, T, Rest, [H|Acc]);
p2_eq(S,     [],    Rest, Acc) -> p2(S, Rest, Acc).

p2_ins(S, [H|T], Rest, Acc) -> p2_ins(S, T, Rest, [H|Acc]);
p2_ins(S, [],    Rest, Acc) -> p2(S, Rest, Acc).

p2_del([H|S], [H|T], Rest, Acc) -> p2_del(S, T, Rest, Acc);
p2_del(S,     [],    Rest, Acc) -> p2(S, Rest, Acc).


t_final_script(no_tracer, _)       -> ok;
t_final_script(Tracer, EditScript) -> Tracer({final_edit_script, EditScript}).

t_d(no_tracer, _) -> ok;
t_d(Tracer, D)    -> Tracer({d,D}).

t_dpath(no_tracer, _)  -> ok;
t_dpath(Tracer, DPath) -> Tracer({dpath,DPath}).

t_exhausted_kdiagonals(no_tracer, _) -> ok;
t_exhausted_kdiagonals(Tracer, D)    -> Tracer({exhausted_kdiagonals, D}).


%% ----- TESTS ------------------------------------------------
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

whitespace_1_test() ->
    [{eq,"A"},{ins," "},{eq,"/"},{ins," "},{eq,"B"},{del," "}] =
        diff("A/B ", "A / B").

whitespace_1r_test() ->
    % [{eq,"A"},{del," "},{eq,"/"},{del," "},{eq,"B"},{ins," "}]
    [{eq,"A"},{del," "},{eq,"/"},{ins,"B"},{eq," "},{del,"B"}] =
        diff("A / B", "A/B ").

% whitespace_1ro_test() ->
%     [{eq,"A"},{del," "},{eq,"/"},{del," "},{eq,"B"},{ins," "}] =
%         diff_orig("A / B", "A/B ").

whitespace_2_test() ->
    [{eq,"C("},{ins," "},{eq,"D"},{del," "}] =
        diff("C(D ", "C( D").

whitespace_2r_test() ->
    % [{eq,"C("},{del," "},{eq,"D"},{ins," "}]
    [{eq,"C("},{ins,"D"},{eq," "},{del,"D"}] =
        diff("C( D", "C(D ").

% whitespace_2ro_test() ->
%     [{eq,"C("},{del," "},{eq,"D"},{ins," "}] =
%         diff_orig("C( D", "C(D ").

whitespace_3_test() ->
    [{eq,"E"},{ins," "},{eq,"+"},{del," "},{eq,"F"},{del," "}] =
        diff("E+ F ", "E +F").

whitespace_3r_test() ->
    % [{eq,"E"},{del," "},{eq,"+"},{ins," "},{eq,"F"},{ins," "}]
    [{eq,"E"},{ins,"+"},{eq," "},{del,"+"},{eq,"F"},{ins," "}] =
        diff("E +F", "E+ F ").

% whitespace_3ro_test() ->
%     % [{eq,"E"},{ins,"+"},{eq," "},{del,"+"},{eq,"F"},{ins," "}]
%     [{eq,"E"},{del," "},{eq,"+"},{ins," "},{eq,"F"},{ins," "}] =
%         diff_orig("E +F", "E+ F ").

whitespace_4_test() ->
    [{eq,"E"},{ins,"\t"},{eq,"+"},{del," "},{eq,"F"},{del," "}] =
        diff("E+ F ", "E\t+F").

whitespace_4r_test() ->
    [{eq,"E"},{del,"\t"},{eq,"+"},{ins," "},{eq,"F"},{ins," "}] =
        diff("E\t+F", "E+ F ").

whitespace_5_test() ->
    [{del," "},{eq,"E"},{ins,"\t"},{eq,"+"},{ins,"\nF"},{eq," "},{del,"F"}] =
        diff(" E+ F", "E\t+\nF ").

whitespace_5r_test() ->
    [{ins," "},{eq,"E"},{del,"\t"},{eq,"+"},{del,"\n"},{ins," "},{eq,"F"},{del," "}] =
        diff("E\t+\nF ", " E+ F").

simple_diff_test() ->
    [{eq,"a"},{del,"B"},{ins,"X"},{eq,"ccc"},{del,"D"},{ins,"Y"},{eq,"e"}] =
        diff("aBcccDe", "aXcccYe").

completely_mismatching_test() ->
    [{del,"aaa"}, {ins,"bbb"}] = diff("aaa", "bbb").

empty_inputs_produces_empty_diff_test() ->
    [] = diff("", "").

only_additions_test() ->
    [{ins,"aaa"}] = diff("", "aaa"),
    [{eq,"a"},{ins,"b"},{eq,"a"},{ins,"b"},{eq,"a"},{ins,"b"},{eq,"a"}] =
        diff("aaaa", "abababa").

only_deletions_test() ->
    [{del,"aaa"}] = diff("aaa", ""),
    [{eq,"a"},{del,"b"},{eq,"a"},{del,"b"},{eq,"a"},{del,"b"},{eq,"a"}] =
        diff("abababa", "aaaa").

patch_test() ->
    Diff = diff(Old="a cat ate my hat", New="a dog ate my shoe"),
    New = patch(Old, Diff).

diff_patch_binaries_test() ->
    [{del,["The Naming of Cats is a difficult matter,\n"]},
     {ins,["The Naming of Dogs is a different matter,\n"]},
     {eq,["It isn't just one of your holiday games;\n",
          "You may think at first I'm as mad as a hatter\n"]},
     {del,["When I tell you, a cat must have THREE DIFFERENT NAMES.\n"]}] =
        Diff =
        diff_binaries(
          %% T.S. Elliot:
          Old =
              <<"The Naming of Cats is a difficult matter,\n"
                "It isn't just one of your holiday games;\n"
                "You may think at first I'm as mad as a hatter\n"
                "When I tell you, a cat must have THREE DIFFERENT NAMES.\n">>,
          %% Not T.S. Elliot (of course):
          New =
              <<"The Naming of Dogs is a different matter,\n"
                "It isn't just one of your holiday games;\n"
                "You may think at first I'm as mad as a hatter\n">>),
    New = list_to_binary(patch_binaries(Old, Diff)).

-endif.