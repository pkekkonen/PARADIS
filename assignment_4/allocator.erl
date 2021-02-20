% Re-write the resource allocator we saw in Erlang Lecture 6 to support named
% resources instead of an arbitrary number of resources.
% More specifically, the start/1-function should accept a map of named
% resources Pid = allocator:start(#{a=>10, b=>20, c=>30}). 
% Requesting resources is done through calling request/2 with a list of resources to
% be requested e.g., R = allocator:request(Pid, [a, c]). The call
% to request/2 should block until the resources are available and return
% a map with the resources, i.e., if any of the resources is unavailable the
% function waits until they have been released. Resources are released using allocator:release(Pid, R), where R is a map of resources to be
% released.
% The module shall export start/1, request/2 and release/2 as specified.


-module(allocator).
% -export([start/1, request/2, release/2, test2/0, allocate_test/3]).
-export([start/1, request/2, release/2, test2/0, allocate_test/3]).

start(Resources) ->
    spawn_link(fun () ->
		       allocator(Resources)
	       end).

request(Pid, N) ->
    Ref = make_ref(),
    Pid ! {request, {self(), Ref, N}},
    receive
	{granted, Ref, Granted} ->
	    Granted
    end.

release(Pid, Resources) ->
    Ref = make_ref(),
    Pid ! {release, {self(), Ref, Resources}},
    receive
	{released, Ref} ->
	    ok
    end.


allocator(Resources) ->
    receive
    	%TODO: här 
	{request, {Pid, Ref, N}} ->
		case enough_resources(N, Resources) of
			true ->
	    		Pid ! {granted, Ref, convert_to_map(N)},
	    		allocator(remove_from_map(N, Resources));
			false ->
				allocator(Resources)
		end;
	{release, {Pid, Ref, Released}} ->
	    Pid ! {released, Ref},
	    allocator(maps:fold(fun(K, V, Map) -> maps:update_with(K, fun(X) -> X + V end, V, Map) end, Resources, Released))
    end.

remove_from_map([], Map) -> Map;
remove_from_map([H|T], Map) ->
	remove_from_map(T, maps:put(H, maps:get(H, Map), Map)).


enough_resources([Current|Tail], Resources) when is_map_key(Current, Resources) ->
	case maps:get(Current, Resources) of
		0 -> false;
		_ ->
			case Tail of
				[] -> true;
				_T -> enough_resources(Tail, maps:put(Current, maps:get(Current, Resources)-1, Resources))
			end
	end;
enough_resources(_N, _Resources) ->
	false.

convert_to_map(List) -> convert_to_map(List, #{}).
convert_to_map([], Result) -> Result;
convert_to_map([Head|Tail], Result) -> 
	case is_map_key(Head, Result) of
		true -> convert_to_map(Tail, maps:put(Head, maps:get(Head, Result)+1, Result));
		false -> convert_to_map(Tail, maps:put(Head, 1, Result))
	end.

			     
test2() ->
   	Allocator = allocator:start(#{a=>2, b=>2, c=>3}),
    spawn(?MODULE, allocate_test, [Allocator, "Process A", [a, c]]),
    spawn(?MODULE, allocate_test, [Allocator, "Process B", [a,b,b,c,a]]),
    spawn(?MODULE, allocate_test, [Allocator, "Process C", [c,c,c,b,a,b,a]]).

allocate_test(Allocator, Name, N) ->    
    io:format("~p requests ~p resources ~n", [Name, N]),
    S = allocator:request(Allocator, N),
    receive after 2000 -> ok end,
    io:format("~p releasing ~p~n", [Name, S]),
    allocator:release(Allocator, S),
    allocate_test(Allocator, Name, N).
