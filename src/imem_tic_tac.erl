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
        ]).

% sample usage for demo:
% imem_tic_tac:start(3,3,3,0,0,"X","O",undefined,false,false).
% imem_tic_tac:start(4,4,4,2,2,"X","O",undefined,false,false).
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
    case play_bot(Player, State) of
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
win(Board, 4, 4, 4, false, Player) -> win_444(Board, Player);
win(Board, 4, 4, 3, false, Player) -> win_443(Board, Player);
win(Board, 5, 5, 5, false, Player) -> win_555(Board, Player);
win(Board, 5, 5, 4, false, Player) -> win_554(Board, Player);
win(Board, 5, 5, 3, false, Player) -> win_553(Board, Player);
win(_Board, _Width, _Height, _Run, _Periodic, _Player) -> false. % TODO: implement all board sizes

win_555(<<       P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:40,  P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:80,  P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:120, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<_:160, P:8, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_555(<<       P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:8,   P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:16,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:24,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<_:32,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_555(<<P:8, _:40, P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_555(<<_:32, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_555(_, _) -> false.  % TODO: implement other win patterns

win_554(<<       P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:8,   P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:40,  P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:48,  P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:80,  P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:88,  P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:120, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:128, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:160, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<_:168, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_554(<<       P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:8,   P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:16,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:24,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:32,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:40,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:48,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:56,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:64,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<_:72,  P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_554(<<       P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_554(<<_:8,   P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_554(<<_:40,  P:8, _:40, P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_554(<<_:24,  P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_554(<<_:32,  P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_554(<<_:72,  P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_554(_, _) -> false.  % TODO: implement other win patterns

% gen(W,H,R) -> 
%     lists:flatten([ gen_hor(W,H,R,H,W-R,[])
%                   , gen_ver(W,H,R,W,H-R,[])
%                   , gen_bck(W,H,R,W-R,H-R,[])
%                   , gen_fwd(W,H,R,W-R,H-R,[])
%                   ]).

% gen_hor(_,_,_,-1,-1,Acc) -> Acc; 
% gen_hor(X,Y,R,Y,O,Acc) -> Acc; 


win_553(<<       P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:8,   P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:16,  P:8, P:8, P:8, _/binary>>, P) -> true;

win_553(<<_:40,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:48,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:56,  P:8, P:8, P:8, _/binary>>, P) -> true;

win_553(<<_:80,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:88,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:96,  P:8, P:8, P:8, _/binary>>, P) -> true;

win_553(<<_:120, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:128, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:136, P:8, P:8, P:8, _/binary>>, P) -> true;

win_553(<<_:160, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:168, P:8, P:8, P:8, _/binary>>, P) -> true;
win_553(<<_:176, P:8, P:8, P:8, _/binary>>, P) -> true;

win_553(<<       P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:8,   P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:16,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:24,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:32,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:40,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:48,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:56,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:64,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:72,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:80,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:88,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:96,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:104, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_553(<<_:112, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;

win_553(<<       P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:8,   P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:16,  P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:40,  P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:48,  P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:56,  P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:80,  P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:88,  P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;
win_553(<<_:96,  P:8, _:40, P:8, _:40, P:8, _/binary>>, P) -> true;

win_553(<<_:16,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:24,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:32,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:56,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:64,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:72,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:136, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:144, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(<<_:152, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_553(_, _) -> false.  % TODO: implement other win patterns

win_444(<<      P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_444(<<_:32, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_444(<<_:64, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_444(<<_:92, P:8, P:8, P:8, P:8, _/binary>>, P) -> true;
win_444(<<      P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_444(<<_:8,  P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_444(<<_:16, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_444(<<_:24, P:8, _:24, P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_444(<<      P:8, _:32, P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_444(<<_:24, P:8, _:16, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_444(_, _) -> false.  % TODO: implement other win patterns

win_443(<<       P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:8,   P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:32,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:40,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:64,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:72,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:92,  P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<_:100, P:8, P:8, P:8, _/binary>>, P) -> true;
win_443(<<       P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:8,   P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:16,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:24,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:32,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:40,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:48,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<_:56,  P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_443(<<       P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_443(<<_:8,   P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_443(<<_:32,  P:8, _:32, P:8, _:32, P:8, _/binary>>, P) -> true;
win_443(<<_:16,  P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_443(<<_:24,  P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_443(<<_:56,  P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_443(_, _) -> false.  % TODO: implement other win patterns

win_333(<<      P:8, P:8, P:8, _/binary>>, P) -> true;
win_333(<<_:24, P:8, P:8, P:8, _/binary>>, P) -> true;
win_333(<<_:48, P:8, P:8, P:8, _/binary>>, P) -> true;
win_333(<<      P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_333(<<_:8,  P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_333(<<_:16, P:8, _:16, P:8, _:16, P:8, _/binary>>, P) -> true;
win_333(<<P:8, _:24, P:8, _:24, P:8, _/binary>>, P) -> true;
win_333(<<_:16, P:8, _:8, P:8, _:8, P:8, _/binary>>, P) -> true;
win_333(_Board, _Player) -> false. 


play_bot(Player, #state{board=Board, width=Width, height=Height, next=Player, gravity=Gravity} = State) ->
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
    put(Board, lists:nth(random:uniform(length(Options)), Options), Player);
play_bot_random(#state{next=Player, board=Board, width=Width, gravity=true}, Options) ->
    gravity_put(Board, Width, lists:nth(random:uniform(length(Options)), Options), Player).

play_bot_immediate_win(_State, _Options) -> todo.

play_bot_defend_immediate(_State, _Options) -> todo.

put_options(Board, Width, Height, false) ->
    lists:usort([ case B of ?AVAILABLE -> I; _ -> false end || {B,I} <- lists:zip(binary_to_list(Board), lists:seq(0,Width*Height-1))]) -- [false];
put_options(Board, Width, _Height, true) ->
    lists:usort([ case B of ?AVAILABLE -> I; _ -> false end || {B,I} <- lists:zip(binary_to_list(binary:part(Board,0,Width)), lists:seq(0,Width-1))]) -- [false].







 