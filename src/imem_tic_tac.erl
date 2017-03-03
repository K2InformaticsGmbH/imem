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
-define(BLOCK, $$).
-define(JOKER, $*).
-define(AVAILABLE,32).       % space

-record(state,
                { width      :: integer()   % board width >= 3
                , height     :: integer()   % board height >= 3
                , run        :: integer()   % sucess run length
                , jokers     :: list()      % cells which can be used by both players
                , blocks     :: list()      % blocked cells, list of integers, e.g. [12,13]
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
        ]).

% sample usage for demo:
% imem_tic_tac:start(3,3,3,0,0,"X","O",undefined,false,false).
% imem_tic_tac:start(5,4,4,2,2,"X","O",undefined,false,false).
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

start(Width, Height, Run, Jokers, Blocks, Starter, Other, Bot, Gravity, Periodic) ->
    {J,B} = case is_list(Jokers) of
        true ->     JJ = cells_to_integer_list(Width, Height, Jokers, []),
                    BB = cells_to_integer_list(Width, Height, Blocks, JJ),
                    {JJ,BB};
        false ->    BB = cells_to_integer_list(Width, Height, Blocks, []),
                    JJ = cells_to_integer_list(Width, Height, Jokers, BB),
                    {JJ,BB}
    end,
    Params = [ Width
             , Height
             , Run
             , J
             , B
             , player_to_integer(Starter)
             , player_to_integer(Other)
             , player_to_integer(Bot)
             , Gravity
             , Periodic
             ],
    ChildSpec = { imem_tic_tac                          % ChildId
                , {imem_tic_tac,start_link,[Params]}  % {M,F,A}
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

cell_to_integer(Cell, _Width, _Height) when is_integer(Cell) -> Cell; 
cell_to_integer(Cell, Width, _Height) when is_atom(Cell) -> 
    case atom_to_list(Cell) of
        [C] ->      C-hd(?COLS);
        [C,R] ->    C-hd(?COLS) + Width * (R-hd(?ROWS));
        _ ->        -1
    end. 

cells_to_integer_list(_Width, _Height, 0, _) -> [];
cells_to_integer_list(Width, Height, Cells, Occupied) when is_integer(Cells) -> 
    random:seed(erlang:now()),
    random_cells(Width, Height, Cells, Occupied, []);
cells_to_integer_list(_Width, _Height, [], _) -> [];
cells_to_integer_list(Width, Height, Cells, _) when is_list(Cells), is_atom(hd(Cells)) -> 
    lists:usort([cell_to_integer(A, Width, Height) || A <- Cells]);
cells_to_integer_list(_Width, _Height, Cells, _) when is_list(Cells), is_integer(hd(Cells)) -> 
    lists:usort(Cells).

random_cells(_Width, _Height, 0, _Occupied, Acc) -> lists:sort(Acc); 
random_cells(Width, Height, N, Occupied, Acc) -> 
    X = random:uniform(Width*Height),
    case {is_member(X, Acc), is_member(X, Occupied)} of
        {false,false} ->    random_cells(Width, Height, N-1, Occupied, [X|Acc]);
        _ ->                random_cells(Width, Height, N, Occupied, Acc)
    end.

init([Width, Height, Run, Jokers, Blocks, Starter, Other, Bot, Gravity, Periodic]) ->
    Board0 = list_to_binary(lists:duplicate(Width*Height,?AVAILABLE)),
    Board1 = board_replace(Board0, Blocks, ?BLOCK),
    Board2 = board_replace(Board1, Jokers, ?JOKER),
    State = #state  { width=Width
                    , height=Height
                    , run=Run
                    , jokers=Jokers
                    , blocks=Blocks
                    , starter=Starter
                    , other=Other
                    , bot=Bot
                    , gravity=Gravity
                    , periodic=Periodic
                    , board=Board2 
                    , next=Starter
                    },
    print_all(State),
    {ok, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
handle_cast(_Request, State) -> {noreply, State}.
handle_info(_, State) -> {noreply, State}.

handle_call(state, _From, State) ->
    ?Info("state ~p",[State]),
    {reply, ok, State};
handle_call(print, _From, State) ->
    print_all(State),
    {reply, ok, State};
handle_call({play, Cell}, _From, #state{next=Player} = State) ->
    handle_call({play, Player, Cell}, _From, State);
handle_call({play, Player, Cell}, _From, #state{width=Width, height=Height, run=Run, next=Player, starter=Starter, other=Other, board=Board, gravity=Gravity, periodic=Periodic} = State) ->
    case cell_to_integer(Cell, Width, Height) of
        I when I<0;I>=Width*Height ->
            {reply, bad_cell_coordinate, State};
        I ->
            Idx = case Gravity of
                false ->    I;
                true ->     gravity_fall(I rem Width, Board, Width)
            end,
            case binary:at(Board, Idx) of
                ?AVAILABLE ->   
                    NewBoard = board_put(Board, Idx, Player),
                    NewState = State#state{next=next_player(Player, Starter, Other), board=NewBoard},
                    case is_win_raw(NewBoard, Width, Height, Run, Player, Periodic) of
                        true -> 
                            print_all(NewState, true),
                            {stop, normal, NewState};
                        false ->
                            print_all(NewState, false),
                            {reply, ok, NewState}
                    end;
                _ ->
                    {reply, already_occupied, State}
            end
    end;
handle_call({play, _, _}, _From, State) ->
    {reply, not_your_turn, State};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

gravity_fall(I, Board, Width) when I + Width >= size(Board) -> I;
gravity_fall(I, Board, Width) ->
    case {binary:at(Board, I), binary:at(Board, I+Width)} of
        {?AVAILABLE,?AVAILABLE} -> gravity_fall(I+Width, Board, Width);
        _ -> I
    end.

board_put(Board, Idx, NewVal) when is_integer(Idx), is_integer(NewVal) ->
    Prefix = binary:part(Board, 0, Idx),
    Suffix = binary:part(Board, Idx+1, size(Board)-Idx-1),
    <<Prefix/binary, NewVal:8, Suffix/binary>>.

board_replace(Board, [], _) -> Board;
board_replace(Board, [Idx|Rest], Val) -> 
    board_replace(board_put(Board, Idx, Val), Rest, Val).

is_member(_, []) -> false;
is_member(C, List) -> lists:member(C, List).


next_player(Starter,Starter,Other) -> Other;
next_player(Other,Starter,Other) -> Starter.

print_all(State) -> print_all(State, false).

print_all(#state{next=Next, bot=Bot} = State, false) ->
    print_row(0,State),
    case Next of
        Bot ->  ?Info("next move ~s (bot)",[[Next]]);
        _ ->    ?Info("next move ~s",[[Next]])
    end;
print_all(State, true) ->
    print_row(0,State),
    ?Info("Congratulations, you  won").

print_row(N,#state{height=Height}) when N>Height -> ok;
print_row(0, #state{width=Width} = State) -> 
    ?Info("~s",[""]),
    ?Info("board ~s",[" |" ++ lists:sublist(?COLS,Width) ++ "|"]),
    print_row(1,State);
print_row(N, State) -> 
    ?Info("      ~s",[row_str(N,State)]),
    print_row(N+1,State).

row_str(N,#state{width=Width, board=Board}) ->
    [lists:nth(N, ?ROWS),$|] ++ binary_to_list(binary:part(Board, (N-1) * Width, Width)) ++ "|".

is_win_raw(Board, Width, Height, Run, Player, Periodic) -> 
    is_win(binary:replace(Board, <<?JOKER:8>>, <<Player:8>>), Width, Height, Run, Player, Periodic).

-spec is_win(Board::binary(), Width::integer(), Height::integer(), Run::integer(), Player::integer(), Periodic::boolean()) -> boolean().
is_win(Board, 3, 3, 3, Player, false) -> is_win_333(Board, Player);
is_win(Board, 4, 4, 4, Player, false) -> is_win_444(Board, Player);
is_win(_Board, _Width, _Height, _Run, _Player, _Periodic) -> false. % TODO: implement all board sizes

is_win_444(<<P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
is_win_444(_, _) -> false.  % TODO: implement other win patterns

is_win_333(<<P:8, P:8, P:8, _/binary>>, P) -> true;
is_win_333(<<_:24, P:8, P:8, P:8, _/binary>>, P) -> true;
is_win_333(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
is_win_333(<<P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
is_win_333(<<_:8, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
is_win_333(<<_:16, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
is_win_333(<<P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
is_win_333(<<_:16, P:8, _:8, P:8, _:8, P:8, _/binary>>, P) -> true;
is_win_333(_Board, _Player) -> false. 









 