-module(imem_tic_tac).

-include("imem.hrl").           % import logging macros
-include("imem_meta.hrl").      % import exceptions and data types

-behavior(gen_server).          % implicitly defines the required callbacks

-record(ddTicTac,   % score table cache
                  { board   :: binary()  % {width x height}
                  , score   :: term()
                  }
       ).
-define(ddTicTac, [binary,term]).  
-define(ddTestName,ddTicTac).

-define(COLS, "abcdefgh").
-define(ROWS, "12345678").
-define(OBSTACLE, $$).
-define(JOKER, $*).
-define(AVAILABLE,32).       % space

-record(state,
                { width      :: integer()   % board width >= 3
                , height     :: integer()   % board height >= 3
                , run        :: integer()   % sucess run length
                , starter    :: integer()   % starting player (capital ascii)
                , other      :: integer()   % other player (capital ascii)
                , bot        :: integer()   % machine player Starter|Other|AVAIL = none
                , gravity    :: boolean()   % do moves fall towards higher row numbers
                , periodic   :: boolean()   % unbounded repeating board 
                , board      :: binary()    % 
                , next       :: integer()   % Starter|Other
                }
       ).

% gen_server API call exports

-export([ start/10
        , stop/0
        , play/1
        , play/2
        , state/0
        , print/0
        , gen/3
        ]).

% sample usage for demo:
% plain tic-tac-toe
% imem_tic_tac:start(3,3,3,0,0,"X","O",undefined,false,false).
% 4x4 tic-tac-toe against bot "O"
% imem_tic_tac:start(4,4,4,1,1,"X","O","O",false,false).
% plain four wins
% imem_tic_tac:start(7,6,4,0,0,"X","O","O",false,false).
% imem_tic_tac:play(a1).
% imem_tic_tac:stop().

% gen_server behavior callback exports

-export([ start_link/1
        , init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

start(Width, Height, Run, Obstacles, Jokers, Starter, Other, Bot, Gravity, Periodic) ->
    Board0 = list_to_binary(lists:duplicate(Width*Height,?AVAILABLE)),
    {ok, Board1} = case {Gravity, is_list(Obstacles)} of
        {false,true} -> put_multi(Board0, cells_to_integer_list(Width, Height, Obstacles), ?OBSTACLE);
        {true,true} ->  gravity_put_multi(Board0, Width, cells_to_integer_list(Width, Height, Obstacles), ?OBSTACLE);
        _ ->            {ok, Board0}
    end, 
    {ok, Board2} = case {Gravity, is_list(Jokers)} of
        {false,true} -> put_multi(Board1, cells_to_integer_list(Width, Height, Jokers), ?JOKER);
        {true,true} ->  gravity_put_multi(Board1, Width, cells_to_integer_list(Width, Height, Jokers), ?JOKER);
        _ ->            {ok, Board1}
    end,
    {ok, Board3} = case {Gravity, is_integer(Obstacles)} of
        {false,true} -> put_random(Board2, Obstacles, ?OBSTACLE);
        {true,true} ->  gravity_put_random(Board2, Width, Obstacles, ?OBSTACLE);
        _ ->            {ok, Board2}
    end, 
    {ok, Board4} = case {Gravity, is_integer(Jokers)} of
        {false,true} -> put_random(Board3, Jokers, ?JOKER);
        {true,true} ->  gravity_put_random(Board3, Width, Jokers, ?JOKER);
        _ ->            {ok, Board3}
    end, 
    Params = [ Width
             , Height
             , Run
             , player_to_integer(Starter)
             , player_to_integer(Other)
             , player_to_integer(Bot)
             , Gravity
             , Periodic
             , Board4
             ],
    ChildSpec = { imem_tic_tac                          % ChildId
                , {imem_tic_tac,start_link,[Params]}    % {M,F,A}
                , permanent                             % Restart strategy
                , 1000                                  % Shutdown timeout
                , worker                                % Type
                , [imem_tic_tac]                        % Modules
                },
    supervisor:start_child(imem_sup, ChildSpec).

stop() ->
    supervisor:terminate_child(imem_sup, imem_tic_tac),
    supervisor:delete_child(imem_sup, imem_tic_tac).

start_link(Params)  ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Params, []).

state() ->
    gen_server:call(?MODULE, state). 

print() ->
    gen_server:call(?MODULE, print). 

play(Cell) -> gen_server:call(?MODULE, {play, Cell}). 

play(Player, Cell) -> gen_server:call(?MODULE, {play, player_to_integer(Player), Cell}). 

player_to_integer(Player) when is_integer(Player) -> Player; 
player_to_integer(Player) when is_atom(Player) -> hd(string:to_upper(atom_to_list(Player))); 
player_to_integer(undefined) -> undefined; 
player_to_integer("") -> undefined; 
player_to_integer(Player) when is_list(Player) -> hd(string:to_upper(Player)). 

cell_to_integer(Cell, Width, Height) when is_integer(Cell), Cell>=0, Cell<Width*Height -> Cell; 
cell_to_integer(Cell, _Width, _Height) when is_integer(Cell) -> {error, invalid_cell}; 
cell_to_integer(Cell, Width, Height) when is_atom(Cell) -> 
    C0 = hd(?COLS),
    R0 = hd(?ROWS),
    case atom_to_list(Cell) of
        [C] when C>=C0,C<C0+Width -> C-C0;
        [C,R] when C>=C0,C<C0+Width,R>=R0,R<R0+Height -> C-C0+Width*(R-R0);
        _ -> {error, invalid_cell}
    end. 

cells_to_integer_list(Width, Height, Cells) -> 
    cells_to_integer_list(Width, Height, Cells, []). 

cells_to_integer_list(_Width, _Height, [], Acc) -> lists:usort(Acc);
cells_to_integer_list(Width, Height, [Cell|Rest], Acc) -> 
    cells_to_integer_list(Width, Height, Rest, [cell_to_integer(Cell, Width, Height)|Acc]).

init([Width, Height, Run, Starter, Other, Bot, Gravity, Periodic, Board]) ->
    random:seed(erlang:now()),
    State = #state  { width=Width
                    , height=Height
                    , run=Run
                    , starter=Starter
                    , other=Other
                    , bot=Bot
                    , gravity=Gravity
                    , periodic=Periodic
                    , board=Board 
                    , next=Starter
                    },
    print_board(State),
    print_next(Starter, Bot),
    invoke_bot_if_due(Starter, Bot),
    {ok, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
handle_cast(_Request, State) -> {noreply, State}.

handle_info({play_bot, Player}, #state{width=Width, height=Height, run=Run, next=Player, starter=Starter, other=Other, bot=Bot, periodic=Periodic} = State) -> 
    case play_bot(State) of
        {ok, NewBoard} ->   
            Next = next_player(Player, Starter, Other),
            NewState = State#state{next=Next, board=NewBoard},
            case is_win(NewBoard, Width, Height, Run, Periodic, Player) of
                true -> 
                    print_board(NewState),
                    print_win(Player),
                    {stop, normal, NewState};
                false ->
                    print_board(NewState),
                    case is_tie(NewBoard, Width, Height, Run, Periodic, Player) of
                        true ->
                            print_tie(),
                            {stop, normal, NewState};
                        false ->
                            print_next(Next, Bot),
                            invoke_bot_if_due(Next, Bot),    
                            {noreply, NewState}
                    end
            end;
        _ -> 
            {noreply, State}
    end;
handle_info(_, State) -> {noreply, State}.

handle_call(state, _From, State) ->
    ?Info("state ~p",[State]),
    {reply, ok, State};
handle_call(print, _From, #state{next=Player, bot=Bot} = State) ->
    print_board(State),
    print_next(Player,Bot),
    {reply, ok, State};
handle_call({play, Cell}, _From, #state{next=Player} = State) ->
    handle_call({play, Player, Cell}, _From, State);
handle_call({play, Player, Cell}, _From, #state{width=Width, height=Height, run=Run, next=Player, starter=Starter, other=Other, bot=Bot, board=Board, gravity=Gravity, periodic=Periodic} = State) ->
    case cell_to_integer(Cell, Width, Height) of
        {error, invalid_cell} -> {reply, invalid_cell, State};
        Idx when is_integer(Idx) ->
            Result = case Gravity of
                false ->    put(Board, Idx, Player);
                true ->     gravity_put(Board, Width, Idx, Player) 
            end,
            case Result of
                {error, already_occupied} -> {reply, already_occupied, State};
                {ok, NewBoard} ->   
                    Next = next_player(Player, Starter, Other),
                    NewState = State#state{next=Next, board=NewBoard},
                    case is_win(NewBoard, Width, Height, Run, Periodic, Player) of
                        true -> 
                            print_board(NewState),
                            print_win(Player),
                            {stop, normal, NewState};
                        false ->
                            print_board(NewState),
                            case is_tie(NewBoard, Width, Height, Run, Periodic, Player) of
                                true ->
                                    print_tie(),
                                    {stop, normal, NewState};
                                false ->
                                    print_next(Next, Bot),
                                    invoke_bot_if_due(Next, Bot),    
                                    {reply, ok, NewState}
                            end
                    end
            end
    end;
handle_call({play, _, _}, _From, State) ->
    {reply, not_your_turn, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

invoke_bot_if_due(Player, Player) -> self()!{play_bot,Player};
invoke_bot_if_due(_, _) -> ok.

-spec gravity_put(binary(), integer(), integer(), integer()) -> {ok,binary()} | {error,atom()}.
gravity_put(Board, Width, Idx, NewVal) ->
    gravity_put(Board, NewVal, lists:seq(size(Board)-Width+(Idx rem Width), (Idx rem Width) , -Width)).

gravity_put(_, _, []) -> {error, already_occupied};
gravity_put(Board, NewVal, [Idx|Rest]) -> 
    case put(Board, Idx, NewVal) of
        {ok, Binary} -> 
            {ok, Binary};
        {error, already_occupied} ->
            gravity_put(Board, NewVal, Rest)
    end.

-spec gravity_put_multi(binary(), list(), integer(), integer()) -> {ok,binary()} | {error,atom()}.
gravity_put_multi(Board, _Width, [], _NewVal) -> {ok, Board};
gravity_put_multi(Board, Width, [Idx|Rest], NewVal) ->
    case gravity_put(Board, Width, Idx, NewVal) of
        {error, already_occupied} -> {error, already_occupied};
        {ok, Binary} ->              gravity_put_multi(Binary, Width, Rest, NewVal)
    end.

-spec gravity_put_random(binary(), integer(), integer(), integer()) -> {ok,binary()} | {error,atom()}.
gravity_put_random(Board, _, 0, _) -> {ok, Board};
gravity_put_random(Board, Width, Count, NewVal) ->
    Idx = random:uniform(Width)-1,
    case gravity_put(Board, Width, Idx, NewVal) of 
        {error, already_occupied} -> gravity_put_random(Board, Width, Count, NewVal);
        {ok, Binary} ->              gravity_put_random(Binary, Width, Count-1, NewVal)
    end.

-spec put(binary(), integer(), integer()) -> {ok,binary()} | {error,atom()}.
put(Board, Idx, NewVal) ->
    case binary:at(Board, Idx) of 
        ?AVAILABLE ->
            Prefix = binary:part(Board, 0, Idx),
            Suffix = binary:part(Board, Idx+1, size(Board)-Idx-1),
            {ok, <<Prefix/binary, NewVal:8, Suffix/binary>>};
        _ ->
            {error, already_occupied}
    end.

-spec put_multi(binary(), list(), integer()) -> {ok,binary()} | {error,atom()}.
put_multi(Board, [], _NewVal) -> {ok, Board};
put_multi(Board, [Idx|Rest], NewVal) ->
    case put(Board, Idx, NewVal) of 
        {error, already_occupied} -> {error, already_occupied};
        {ok, Binary} ->              put_multi(Binary, Rest, NewVal)
    end.

-spec put_random(binary(), integer(), integer()) -> {ok,binary()} | {error,atom()}.
put_random(Board, 0, _) -> {ok, Board};
put_random(Board, Count, NewVal) ->
    Idx = random:uniform(size(Board))-1,
    case put(Board, Idx, NewVal) of 
        {error, already_occupied} -> put_random(Board, Count, NewVal);
        {ok, Binary} ->              put_random(Binary, Count-1, NewVal)
    end.

next_player(Starter,Starter,Other) -> Other;
next_player(Other,Starter,Other) -> Starter.

print_next(Next, Bot) ->
    case Next of
        Bot ->  ?Info("next move ~s (bot)",[[Next]]);
        _ ->    ?Info("next move ~s",[[Next]])
    end.

print_tie() ->
    ?Info("This game ended in a tie."),
    ?Info("").

print_win(Player) ->
    ?Info("Congratulations, ~s won",[[Player]]),
        ?Info("").

print_board(#state{width=Width} = State) -> 
    ?Info("~s",[""]),
    ?Info("board ~s",[" |" ++ lists:sublist(?COLS,Width) ++ "|"]),
    print_row(1,State).

print_row(N,#state{height=Height}) when N>Height -> ok;
print_row(N, State) -> 
    ?Info("      ~s",[row_str(N,State)]),
    print_row(N+1,State).

row_str(N,#state{width=Width, board=Board}) ->
    [lists:nth(N, ?ROWS),$|] ++ binary_to_list(binary:part(Board, (N-1) * Width, Width)) ++ "|".

-spec is_tie(Board::binary(), Width::integer(), Height::integer(), Run::integer(), Periodic::boolean(), Player::integer()) -> boolean().
is_tie(Board, _, _, _, _, _) ->  binary:match(Board, <<?AVAILABLE>>) =:= nomatch.

is_win(Board, Width, Height, Run, Periodic, Player) -> 
    win(binary:replace(Board, <<?JOKER:8>>, <<Player:8>>, [global]), Width, Height, Run, Periodic, Player).

-spec win(Board::binary(), Width::integer(), Height::integer(), Run::integer(), Periodic::boolean(), Player::integer()) -> boolean().
win(Board, 3, 3, 3, false, Player) -> win_333(Board, Player);
win(Board, 4, 3, 3, false, Player) -> win_433(Board, Player);
win(Board, 4, 4, 3, false, Player) -> win_443(Board, Player);
win(Board, 4, 4, 4, false, Player) -> win_444(Board, Player);
win(Board, 5, 3, 3, false, Player) -> win_533(Board, Player);
win(Board, 5, 4, 3, false, Player) -> win_543(Board, Player);
win(Board, 5, 4, 4, false, Player) -> win_544(Board, Player);
win(Board, 5, 5, 3, false, Player) -> win_553(Board, Player);
win(Board, 5, 5, 4, false, Player) -> win_554(Board, Player);
win(Board, 5, 5, 5, false, Player) -> win_555(Board, Player);
win(Board, 6, 3, 3, false, Player) -> win_633(Board, Player);
win(Board, 6, 4, 3, false, Player) -> win_643(Board, Player);
win(Board, 6, 4, 4, false, Player) -> win_644(Board, Player);
win(Board, 6, 5, 3, false, Player) -> win_653(Board, Player);
win(Board, 6, 5, 4, false, Player) -> win_654(Board, Player);
win(Board, 6, 5, 5, false, Player) -> win_655(Board, Player);
win(Board, 6, 6, 3, false, Player) -> win_663(Board, Player);
win(Board, 6, 6, 4, false, Player) -> win_664(Board, Player);
win(Board, 6, 6, 5, false, Player) -> win_665(Board, Player);
win(Board, 6, 6, 6, false, Player) -> win_666(Board, Player);
win(Board, 7, 6, 3, false, Player) -> win_763(Board, Player);
win(Board, 7, 6, 4, false, Player) -> win_764(Board, Player);
win(Board, 7, 6, 5, false, Player) -> win_765(Board, Player);
win(Board, 7, 6, 6, false, Player) -> win_766(Board, Player);
win(Board, 7, 7, 4, false, Player) -> win_774(Board, Player);
win(Board, 7, 7, 5, false, Player) -> win_775(Board, Player);
win(Board, 7, 7, 6, false, Player) -> win_776(Board, Player);
win(Board, 7, 7, 7, false, Player) -> win_777(Board, Player);
win(Board, 8, 6, 4, false, Player) -> win_864(Board, Player);
win(Board, 8, 6, 5, false, Player) -> win_865(Board, Player);
win(Board, 8, 6, 6, false, Player) -> win_866(Board, Player);
win(Board, 8, 7, 6, false, Player) -> win_876(Board, Player);
win(Board, 8, 7, 7, false, Player) -> win_877(Board, Player);
win(Board, 8, 8, 7, false, Player) -> win_887(Board, Player);
win(Board, 8, 8, 8, false, Player) -> win_888(Board, Player);
win(_Board, _Width, _Height, _Run, _Periodic, _Player) -> false. % TODO: implement all board sizes

win_888(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_888(<<_:64, P:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_888(<<_:128, P:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_888(<<_:192, P:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_888(<<_:256, P:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_888(<<_:320, P:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_888(<<_:384, P:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_888(<<_:448, P:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_888(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_888(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_888(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_888(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_888(<<_:32, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_888(<<_:40, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_888(<<_:48, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_888(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_888(<<_:0, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_888(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_888(_, _) -> false.

win_887(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:64, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:72, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:128, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:136, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:192, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:200, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:256, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:264, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:320, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:328, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:384, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:392, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:448, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:456, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_887(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:72, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:80, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:88, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:32, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:96, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:40, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:104, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:48, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:112, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:120, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_887(<<_:0, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_887(<<_:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_887(<<_:8, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_887(<<_:72, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_887(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_887(<<_:112, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_887(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_887(<<_:120, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_887(_, _) -> false.

win_877(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:8, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:64, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:72, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:128, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:136, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:192, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:200, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:256, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:264, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:320, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:328, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:384, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:392, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_877(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_877(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_877(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_877(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_877(<<_:32, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_877(<<_:40, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_877(<<_:48, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_877(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_877(<<_:0, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_877(<<_:8, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_877(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_877(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_877(_, _) -> false.

win_876(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:16, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:64, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:72, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:80, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:128, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:136, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:144, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:192, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:200, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:208, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:256, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:264, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:272, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:320, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:328, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:336, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:384, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:392, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:400, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_876(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:72, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:80, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:88, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:32, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:96, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:40, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:104, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:48, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:112, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:120, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_876(<<_:0, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_876(<<_:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_876(<<_:8, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_876(<<_:72, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_876(<<_:16, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_876(<<_:80, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_876(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_876(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_876(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_876(<<_:112, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_876(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_876(<<_:120, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_876(_, _) -> false.

win_866(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:16, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:64, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:72, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:80, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:128, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:136, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:144, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:192, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:200, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:208, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:256, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:264, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:272, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:320, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:328, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:336, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_866(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_866(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_866(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_866(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_866(<<_:32, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_866(<<_:40, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_866(<<_:48, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_866(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_866(<<_:0, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_866(<<_:8, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_866(<<_:16, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_866(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_866(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_866(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_866(_, _) -> false.

win_865(<<_:0, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:16, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:24, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:64, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:72, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:80, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:88, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:128, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:136, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:144, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:152, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:192, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:200, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:208, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:216, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:256, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:264, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:272, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:280, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:320, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:328, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:336, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:344, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_865(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:72, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:80, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:88, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:32, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:96, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:40, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:104, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:48, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:112, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:120, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_865(<<_:0, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_865(<<_:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_865(<<_:8, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_865(<<_:72, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_865(<<_:16, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_865(<<_:80, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_865(<<_:24, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_865(<<_:88, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_865(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_865(<<_:96, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_865(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_865(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_865(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_865(<<_:112, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_865(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_865(<<_:120, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_865(_, _) -> false.

win_864(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:16, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:24, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:32, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:64, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:72, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:80, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:88, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:96, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:128, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:136, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:144, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:152, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:160, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:192, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:200, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:208, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:216, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:224, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:256, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:264, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:272, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:280, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:288, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:320, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:328, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:336, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:344, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:352, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_864(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:128, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:72, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:136, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:80, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:144, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:88, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:152, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:32, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:96, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:160, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:40, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:104, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:168, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:48, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:112, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:176, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:120, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:184, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_864(<<_:0, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:64, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:128, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:8, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:72, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:136, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:16, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:80, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:144, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:24, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:88, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:152, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:32, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:96, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:160, P:8, _:64, P:8, _:64, P:8, _:64, P:8, _/binary>>, P) -> true;
win_864(<<_:24, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:88, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:152, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:96, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:160, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:168, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:112, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:176, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:120, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(<<_:184, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_864(_, _) -> false.


win_777(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_777(<<_:56, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_777(<<_:112, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_777(<<_:168, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_777(<<_:224, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_777(<<_:280, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_777(<<_:336, P:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_777(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_777(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_777(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_777(<<_:24, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_777(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_777(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_777(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_777(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_777(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_777(_, _) -> false.

win_776(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:56, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:64, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:112, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:120, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:168, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:176, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:224, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:232, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:280, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:288, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:336, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:344, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_776(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:64, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:72, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:24, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:80, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:88, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:96, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_776(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_776(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_776(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_776(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_776(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_776(<<_:96, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_776(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_776(<<_:104, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_776(_, _) -> false.


win_775(<<_:0, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:16, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:56, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:64, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:72, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:112, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:120, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:128, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:168, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:176, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:184, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:224, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:232, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:240, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:280, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:288, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:296, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:336, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:344, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:352, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_775(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:112, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:64, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:120, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:72, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:128, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:24, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:80, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:136, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:88, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:144, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:96, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:152, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:160, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_775(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:112, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:120, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:72, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:128, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_775(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(<<_:88, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(<<_:144, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(<<_:96, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(<<_:152, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(<<_:104, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(<<_:160, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_775(_, _) -> false.


win_774(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:16, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:24, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:56, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:64, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:72, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:80, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:112, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:120, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:128, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:136, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:168, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:176, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:184, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:192, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:224, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:232, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:240, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:248, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:280, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:288, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:296, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:304, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:336, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:344, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:352, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:360, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_774(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:112, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:168, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:64, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:120, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:176, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:72, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:128, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:184, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:24, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:80, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:136, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:192, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:88, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:144, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:200, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:96, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:152, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:208, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:160, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:216, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_774(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:112, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:168, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:120, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:176, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:72, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:128, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:184, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:80, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:136, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:192, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_774(<<_:24, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:80, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:136, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:192, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:88, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:144, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:200, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:96, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:152, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:208, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:104, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:160, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(<<_:216, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_774(_, _) -> false.


win_766(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:8, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:56, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:64, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:112, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:120, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:168, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:176, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:224, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:232, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:280, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:288, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_766(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_766(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_766(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_766(<<_:24, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_766(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_766(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_766(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_766(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_766(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_766(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_766(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_766(_, _) -> false.

win_765(<<_:0, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:16, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:56, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:64, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:72, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:112, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:120, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:128, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:168, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:176, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:184, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:224, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:232, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:240, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:280, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:288, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:296, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_765(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:64, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:72, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:24, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:80, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:88, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:96, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_765(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_765(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_765(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_765(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_765(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_765(<<_:72, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_765(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_765(<<_:88, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_765(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_765(<<_:96, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_765(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_765(<<_:104, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_765(_, _) -> false.


win_764(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:16, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:24, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:56, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:64, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:72, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:80, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:112, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:120, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:128, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:136, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:168, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:176, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:184, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:192, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:224, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:232, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:240, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:248, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:280, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:288, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:296, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:304, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_764(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:112, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:64, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:120, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:72, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:128, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:24, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:80, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:136, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:32, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:88, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:144, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:40, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:96, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:152, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:160, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_764(<<_:0, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:56, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:112, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:8, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:64, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:120, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:16, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:72, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:128, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:24, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:80, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:136, P:8, _:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_764(<<_:24, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:80, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:136, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:88, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:144, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:96, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:152, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:104, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(<<_:160, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_764(_, _) -> false.

win_763(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:16, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:24, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:32, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:56, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:64, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:72, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:80, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:88, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:112, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:120, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:128, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:136, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:144, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:168, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:176, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:184, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:192, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:200, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:224, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:232, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:240, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:248, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:256, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:280, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:288, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:296, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:304, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:312, P:8, P:8, P:8, _/binary>>, P) -> true;
win_763(<<_:0, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:56, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:112, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:168, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:8, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:64, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:120, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:176, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:16, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:72, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:128, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:184, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:24, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:80, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:136, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:192, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:32, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:88, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:144, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:200, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:40, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:96, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:152, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:208, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:104, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:160, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:216, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_763(<<_:0, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:56, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:112, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:168, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:8, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:64, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:120, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:176, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:16, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:72, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:128, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:184, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:24, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:80, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:136, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:192, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:32, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:88, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:144, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:200, P:8, _:56, P:8, _:56, P:8, _/binary>>, P) -> true;
win_763(<<_:16, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:72, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:128, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:184, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:24, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:80, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:136, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:192, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:32, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:88, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:144, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:200, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:96, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:152, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:208, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:48, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:104, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:160, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(<<_:216, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_763(_, _) -> false.


win_666(<<_:0, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_666(<<_:48, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_666(<<_:96, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_666(<<_:144, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_666(<<_:192, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_666(<<_:240, P:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_666(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_666(<<_:8, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_666(<<_:16, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_666(<<_:24, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_666(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_666(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_666(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_666(<<_:40, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_666(_, _) -> false.

win_665(<<_:0, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:48, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:56, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:96, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:104, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:144, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:152, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:192, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:200, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:240, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:248, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_665(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:8, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:56, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:16, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:64, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:24, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:72, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:80, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:88, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_665(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_665(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_665(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_665(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_665(<<_:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_665(<<_:80, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_665(<<_:40, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_665(<<_:88, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_665(_, _) -> false.

win_664(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:16, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:48, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:56, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:64, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:96, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:104, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:112, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:144, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:152, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:160, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:192, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:200, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:208, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:240, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:248, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:256, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_664(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:96, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:8, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:56, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:104, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:16, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:64, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:112, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:24, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:72, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:120, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:80, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:128, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:88, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:136, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_664(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:96, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:104, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:64, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:112, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_664(<<_:24, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(<<_:72, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(<<_:120, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(<<_:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(<<_:80, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(<<_:128, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(<<_:40, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(<<_:88, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(<<_:136, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_664(_, _) -> false.

win_663(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:16, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:24, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:56, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:64, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:72, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:96, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:104, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:112, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:120, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:144, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:152, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:160, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:168, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:192, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:200, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:208, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:216, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:240, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:248, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:256, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:264, P:8, P:8, P:8, _/binary>>, P) -> true;
win_663(<<_:0, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:48, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:96, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:144, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:8, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:56, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:104, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:152, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:16, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:64, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:112, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:160, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:24, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:72, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:120, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:168, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:32, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:80, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:128, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:176, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:88, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:136, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:184, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_663(<<_:0, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:96, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:144, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:8, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:56, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:104, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:152, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:16, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:64, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:112, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:160, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:24, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:72, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:120, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:168, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_663(<<_:16, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:64, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:112, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:160, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:24, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:72, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:120, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:168, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:80, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:128, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:176, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:40, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:88, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:136, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(<<_:184, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_663(_, _) -> false.

win_655(<<_:0, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:8, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:48, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:56, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:96, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:104, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:144, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:152, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:192, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:200, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_655(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_655(<<_:8, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_655(<<_:16, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_655(<<_:24, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_655(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_655(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_655(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_655(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_655(<<_:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_655(<<_:40, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_655(_, _) -> false.

win_654(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:16, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:48, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:56, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:64, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:96, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:104, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:112, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:144, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:152, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:160, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:192, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:200, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:208, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_654(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:8, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:56, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:16, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:64, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:24, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:72, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:80, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:88, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_654(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_654(<<_:48, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_654(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_654(<<_:56, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_654(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_654(<<_:64, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_654(<<_:24, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_654(<<_:72, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_654(<<_:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_654(<<_:80, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_654(<<_:40, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_654(<<_:88, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_654(_, _) -> false.

win_653(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:16, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:24, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:56, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:64, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:72, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:96, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:104, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:112, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:120, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:144, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:152, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:160, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:168, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:192, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:200, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:208, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:216, P:8, P:8, P:8, _/binary>>, P) -> true;
win_653(<<_:0, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:48, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:96, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:8, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:56, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:104, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:16, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:64, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:112, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:24, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:72, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:120, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:32, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:80, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:128, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:88, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:136, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_653(<<_:0, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:96, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:8, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:56, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:104, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:16, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:64, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:112, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:24, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:72, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:120, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_653(<<_:16, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:64, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:112, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:24, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:72, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:120, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:80, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:128, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:40, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:88, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(<<_:136, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_653(_, _) -> false.


win_644(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:16, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:48, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:56, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:64, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:96, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:104, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:112, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:144, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:152, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:160, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_644(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_644(<<_:8, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_644(<<_:16, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_644(<<_:24, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_644(<<_:32, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_644(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_644(<<_:0, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_644(<<_:8, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_644(<<_:16, P:8, _:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_644(<<_:24, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_644(<<_:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_644(<<_:40, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_644(_, _) -> false.

win_643(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:16, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:24, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:56, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:64, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:72, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:96, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:104, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:112, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:120, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:144, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:152, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:160, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:168, P:8, P:8, P:8, _/binary>>, P) -> true;
win_643(<<_:0, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:48, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:8, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:56, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:16, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:64, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:24, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:72, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:32, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:80, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:88, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_643(<<_:0, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_643(<<_:48, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_643(<<_:8, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_643(<<_:56, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_643(<<_:16, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_643(<<_:64, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_643(<<_:24, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_643(<<_:72, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_643(<<_:16, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_643(<<_:64, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_643(<<_:24, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_643(<<_:72, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_643(<<_:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_643(<<_:80, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_643(<<_:40, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_643(<<_:88, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_643(_, _) -> false.

win_633(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:16, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:24, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:56, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:64, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:72, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:96, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:104, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:112, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:120, P:8, P:8, P:8, _/binary>>, P) -> true;
win_633(<<_:0, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_633(<<_:8, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_633(<<_:16, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_633(<<_:24, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_633(<<_:32, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_633(<<_:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_633(<<_:0, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_633(<<_:8, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_633(<<_:16, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_633(<<_:24, P:8, _:48, P:8, _:48, P:8, _/binary>>, P) -> true;
win_633(<<_:16, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_633(<<_:24, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_633(<<_:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_633(<<_:40, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_633(_, _) -> false.

win_555(<<_:0, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:40, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:80, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:120, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:160, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:0, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:8, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:16, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:24, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_555(<<_:32, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_555(_, _) -> false.

win_554(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:40, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:48, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:80, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:88, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:120, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:128, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:160, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:168, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:0, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:40, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:8, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:48, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:16, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:56, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:24, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:64, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:72, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_554(<<_:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_554(<<_:8, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_554(<<_:48, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_554(<<_:24, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_554(<<_:64, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_554(<<_:32, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_554(<<_:72, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_554(_, _) -> false.

win_553(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:16, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:40, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:56, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:80, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:88, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:96, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:120, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:128, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:136, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:160, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:168, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:176, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:0, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:40, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:80, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:8, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:48, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:88, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:16, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:56, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:96, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:24, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:64, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:104, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:72, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:112, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:0, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:80, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:8, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:48, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:88, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:16, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:56, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:96, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:16, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:56, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:96, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:64, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:104, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:32, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:72, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:112, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(_, _) -> false.

win_544(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_544(<<_:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_544(<<_:40, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_544(<<_:48, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_544(<<_:80, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_544(<<_:88, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_544(<<_:120, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_544(<<_:128, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_544(<<_:0, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_544(<<_:8, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_544(<<_:16, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_544(<<_:24, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_544(<<_:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_544(<<_:0, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_544(<<_:8, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_544(<<_:24, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_544(<<_:32, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_544(_, _) -> false.

win_543(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:16, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:40, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:56, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:80, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:88, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:96, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:120, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:128, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:136, P:8, P:8, P:8, _/binary>>, P) -> true;
win_543(<<_:0, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:40, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:8, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:48, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:16, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:56, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:24, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:64, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:72, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_543(<<_:0, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_543(<<_:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_543(<<_:8, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_543(<<_:48, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_543(<<_:16, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_543(<<_:56, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_543(<<_:16, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_543(<<_:56, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_543(<<_:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_543(<<_:64, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_543(<<_:32, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_543(<<_:72, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_543(_, _) -> false.

win_533(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:16, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:40, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:56, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:80, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:88, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:96, P:8, P:8, P:8, _/binary>>, P) -> true;
win_533(<<_:0, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_533(<<_:8, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_533(<<_:16, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_533(<<_:24, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_533(<<_:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_533(<<_:0, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_533(<<_:8, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_533(<<_:16, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_533(<<_:16, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_533(<<_:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_533(<<_:32, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_533(_, _) -> false.

win_444(<<_:0, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_444(<<_:32, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_444(<<_:64, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_444(<<_:96, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_444(<<_:0, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_444(<<_:8, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_444(<<_:16, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_444(<<_:24, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_444(<<_:0, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_444(<<_:24, P:8, _:16, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_444(_, _) -> false.

win_443(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:32, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:40, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:64, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:72, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:96, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:104, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:0, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:32, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:8, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:40, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:16, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:48, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:56, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:0, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_443(<<_:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_443(<<_:8, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_443(<<_:40, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_443(<<_:16, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_443(<<_:48, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_443(<<_:24, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_443(<<_:56, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_443(_, _) -> false.

win_433(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_433(<<_:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_433(<<_:32, P:8, P:8, P:8, _/binary>>, P) -> true;
win_433(<<_:40, P:8, P:8, P:8, _/binary>>, P) -> true;
win_433(<<_:64, P:8, P:8, P:8, _/binary>>, P) -> true;
win_433(<<_:72, P:8, P:8, P:8, _/binary>>, P) -> true;
win_433(<<_:0, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_433(<<_:8, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_433(<<_:16, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_433(<<_:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_433(<<_:0, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_433(<<_:8, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_433(<<_:16, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_433(<<_:24, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_433(_, _) -> false.

win_333(<<_:0, P:8, P:8, P:8, _/binary>>, P) -> true;
win_333(<<_:24, P:8, P:8, P:8, _/binary>>, P) -> true;
win_333(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_333(<<_:0, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_333(<<_:8, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_333(<<_:16, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_333(<<_:0, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_333(<<_:16, P:8, _:8, P:8, _:8, P:8, _/binary>>, P) -> true;
win_333(_, _) -> false.

gen(W,H,R) -> io:format("~s~s~s~s~s",[ 
                     gen_hor(W, H, R)
                   , gen_ver(W, H, R)
                   , gen_bck(W, H, R)
                   , gen_fwd(W, H, R)
                   , gen_else(W, H, R)
                   ]).

skip(Pos) -> list_to_binary([$_,$:] ++ integer_to_list(8*Pos) ++ [$,,32]).

gen_hor(W, H, R) -> [g_hor(W, H, R, Y, O) || Y <- lists:seq(0,H-1), O <- lists:seq(0,W-R)]. 

g_hor(W, H, R, Y, O) ->
    WW = W + $0,
    HH = H + $0,
    RR = R + $0,
    SV = skip(Y*W+O), 
    RV = binary:copy(<<"P:8, ">>, R),
    <<"win_", WW:8, HH:8, RR:8, "(<<", SV/binary, RV/binary, "_/binary>>, P) -> true;\n">>.

gen_ver(W, H, R) -> [g_ver(W, H, R, X, O) || X <- lists:seq(0,W-1), O <- lists:seq(0,H-R)]. 

g_ver(W, H, R, X, O) ->
    WW = W + $0,
    HH = H + $0,
    RR = R + $0,
    SV = skip(O*W+X),
    SP = skip(W-1), 
    RV = binary:copy(<<SP/binary, "P:8, ">>, R-1),
    <<"win_", WW:8, HH:8, RR:8, "(<<", SV/binary, "P:8, ", RV/binary, "_/binary>>, P) -> true;\n">>.

gen_bck(W, H, R) -> [g_bck(W, H, R, X, O) || X <- lists:seq(0,W-R), O <- lists:seq(0,H-R)]. 

g_bck(W, H, R, X, O) ->
    WW = W + $0,
    HH = H + $0,
    RR = R + $0,
    SV = skip(O*W+X),
    SP = skip(W), 
    RV = binary:copy(<<SP/binary, "P:8, ">>, R-1),
    <<"win_", WW:8, HH:8, RR:8, "(<<", SV/binary, "P:8, ", RV/binary, "_/binary>>, P) -> true;\n">>.

gen_fwd(W, H, R) -> [g_fwd(W, H, R, X, O) || X <- lists:seq(R-1,W-1), O <- lists:seq(0,H-R)]. 

g_fwd(W, H, R, X, O) ->
    WW = W + $0,
    HH = H + $0,
    RR = R + $0,
    SV = skip(O*W+X),
    SP = skip(W-2), 
    RV = binary:copy(<<SP/binary, "P:8, ">>, R-1),
    <<"win_", WW:8, HH:8, RR:8, "(<<", SV/binary, "P:8, ", RV/binary, "_/binary>>, P) -> true;\n">>.

gen_else(W, H, R) ->
    WW = W + $0,
    HH = H + $0,
    RR = R + $0,
    <<"win_", WW:8, HH:8, RR:8, "(_, _) -> false.\n">>.

play_bot(#state{board=Board, width=Width, height=Height, gravity=Gravity} = State) ->
    Options = put_options(Board, Width, Height, Gravity),
    case play_bot_immediate_win(State, Options) of
        {ok, NewBoard} ->   
            {ok, NewBoard};   % win in this move detected
        _ ->
            case play_bot_defend_immediate(State, Options) of
                {ok, NewBoard} ->   
                    {ok, NewBoard};   % opponent's win in this move detected and taken
                _ ->
                    play_bot_random(State, Options)
            end
    end.

play_bot_random(#state{next=Player, board=Board, gravity=false}, Options) ->
    Idx = lists:nth(random:uniform(length(Options)), Options),
    put(Board, Idx, Player);
play_bot_random(#state{next=Player, board=Board, width=Width, gravity=true}, Options) ->
    gravity_put(Board, Width, lists:nth(random:uniform(length(Options)), Options), Player).

play_bot_immediate_win(_State, []) -> {nok, no_immediate_win};  
play_bot_immediate_win(#state{board=Board, width=Width, height=Height, run=Run, gravity=false, periodic=Periodic, next=Player} = State, [Idx|Rest]) -> 
    {ok, TestBoard} = put(Board, Idx, Player),
    case is_win(TestBoard, Width, Height, Run, Periodic, Player) of
        true ->     {ok, TestBoard};
        false ->    play_bot_immediate_win(State, Rest)
    end;
play_bot_immediate_win(#state{board=Board, width=Width, height=Height, run=Run, gravity=true, periodic=Periodic, next=Player} = State, [Idx|Rest]) -> 
    {ok, TestBoard} = gravity_put(Board, Width, Idx, Player),
    case is_win(TestBoard, Width, Height, Run, Periodic, Player) of
        true ->     {ok, TestBoard};
        false ->    play_bot_immediate_win(State, Rest)
    end.

play_bot_defend_immediate(_State, []) -> {nok, no_immediate_risk};
play_bot_defend_immediate(#state{board=Board, width=Width, height=Height, run=Run, gravity=false, periodic=Periodic, starter=Starter, other=Other, next=Player} = State, [Idx|Rest]) -> 
    Next = next_player(Player, Starter, Other),
    {ok, TestBoard} = put(Board, Idx, Next),
    case is_win(TestBoard, Width, Height, Run, Periodic, Next) of
        true ->     put(Board, Idx, Player);
        false ->    play_bot_defend_immediate(State, Rest)
    end;
play_bot_defend_immediate(#state{board=Board, width=Width, height=Height, run=Run, gravity=true, periodic=Periodic, starter=Starter, other=Other, next=Player} = State, [Idx|Rest]) -> 
    Next = next_player(Player, Starter, Other),
    {ok, TestBoard} = gravity_put(Board, Width, Idx, Next),
    case is_win(TestBoard, Width, Height, Run, Periodic, Next) of
        true ->     gravity_put(Board, Width, Idx, Player);
        false ->    play_bot_defend_immediate(State, Rest)
    end.

put_options(Board, Width, Height, false) ->
    lists:usort([ case B of ?AVAILABLE -> I; _ -> false end || {B,I} <- lists:zip(binary_to_list(Board), lists:seq(0,Width*Height-1))]) -- [false];
put_options(Board, Width, _Height, true) ->
    lists:usort([ case B of ?AVAILABLE -> I; _ -> false end || {B,I} <- lists:zip(binary_to_list(binary:part(Board,0,Width)), lists:seq(0,Width-1))]) -- [false].







 