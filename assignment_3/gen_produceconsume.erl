-module(gen_produceconsume).
-export([start/2, stop/1, produce/2, consume/1]).
-callback handle_produce(State :: term()) ->  NewState :: term().
-callback handle_consume(State :: term()) ->  NewState :: term().

% Initialize the produce consume buffer with Callback which allows 
% a maximum of T tasks in the queue. Return the process identifier of the buffer.
start(Callback, T) ->
	spawn(fun () -> loop([], Callback, T) end).

loop(List, Callback, T) ->
    receive
	{produce, {Pid, Ref, Task}} when length(List) =< T ->
	    Result = Callback:handle_produce(Task),
	    Pid ! {produced, Ref, Result},
	    loop(List ++ [Result], Callback, T);
	{consume, {Pid, Ref}} when length(List) > 0 ->
		[Task|Tail] = List, 
	    Result = Callback:handle_consume(Task),
	    Pid ! {consumed, Ref, Result},
	    loop(Tail, Callback, T)
    end.

% Stop Pid.
stop(Pid) ->
	exit(Pid, ok).

% Call Callback:handle produce(T) and
% put the result in the queue for processing.
produce(Pid, T) ->
    Ref = make_ref(),
    Pid ! {produce, {self(), Ref, T}},
    receive
	{produced, Ref, Result} ->
	    Result
    end.


% Consume the next work in the queue using Callback:handle consume/1
consume(Pid) ->
    Ref = make_ref(),
    Pid ! {consume, {self(), Ref}},
    receive
	{consumed, Ref, Result} ->
	    Result
    end.