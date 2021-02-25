
-module(barrier).
-export([start/1, wait/2, test/0, do_a/0, do_b/0, do_c/0, do_more/1]).

start(WaitFor) ->
    spawn_link(fun () -> loop(lists:sort(WaitFor), [], []) end).

loop(WaitFor, PidRefs, Refs) when WaitFor =:= Refs ->
				io:format("done ~n"),
    [Pid ! {continue, Ref} || {Pid, Ref} <- PidRefs],
    loop(WaitFor, [], []);
loop(WaitFor, PidRefs, Refs) ->
    receive
	{arrive, {Pid, Ref}} ->
		case lists:member(Ref, WaitFor) of
			true ->
				io:format("t ~n"),
				loop(WaitFor, [{Pid, Ref}|PidRefs], lists:sort([Ref|Refs]));
			false ->
				io:format("f ~n"),
				Pid ! {continue, Ref},		
	    		loop(WaitFor, PidRefs, Refs)
	    end
    end.

wait(Barrier, Ref) ->
    Barrier ! {arrive, {self(), Ref}},
    receive
	{continue, Ref} ->
	    ok
    end.




test() ->
	A = make_ref(), B = make_ref(), C = make_ref(),
	Barrier = start([B, C]),
	spawn(fun () -> do_a(), wait(Barrier, A), do_more(a) end),
	spawn(fun () -> do_b(), wait(Barrier, B), do_more(b) end),
	spawn(fun () -> do_c(), wait(Barrier, C), do_more(c) end).


do_a() -> io:format("Do A ~n", []).
do_b() -> io:format("Do B ~n", []).
do_c() -> io:format("Do C ~n", []).

do_more(X) -> io:format("Do ~w ~n", [X]).















	    
