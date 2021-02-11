% Re-write the barrier module we saw in Erlang Lecture 6 to not expect a
% fixed number of process to wait for. Instead, the barrier should wait until
% a specified set of processes have reached the barrier. All other processes
% are allowed through the barrier without having to wait. In Example 1.1,
% barrier:wait/2 should block until all processes that use [A, B] (i.e.,
% the references used in barrier:start/1) are waiting. The barrier should
% not wait for other processes. As a result, do a() and do b() are guaranteed
% to both have completed before do more(a) and do more(b), whereas
% do more(c) will run as soon as do c() has completed.

% More specifically, barrier should export start/1 and wait/2, where
% start(Refs) should accept as argument a list, Refs, of references that
% should wait at the barrier. Next, wait(Barrier, Ref) should accept
% as the first argument the Pid returned by barrier:start/1 and as the
% second argument a reference. If Ref is in Refs, wait/2 should block until
% all references in Refs has arrived to wait/2. If Ref is not in Refs, the
% process is let through the barrier


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















	    
