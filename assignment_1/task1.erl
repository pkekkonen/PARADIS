-module(task1).
-export([eval/1, eval/2, map/2, filter/2, split/2, groupby/2]).

% Problem 1

eval(T) ->
	try
		{ok, eval_helper(T, #{})}
	catch
		_:_ -> error
	end.

% Problem 2

eval(E, L) -> 
	try
		{ok, eval_helper(E,L)}
	catch
		variable_not_found -> {error, variable_not_found};
		_:_ -> {error, unknown_error}
	end.

% Problem 1+2 helper function

eval_helper(E, L) -> 
	try
		case E of
			N when is_number(N) -> N;
			{_, A, _} when (is_atom(A) andalso is_map_key(A, L) =:= false) -> throw(variable_not_found);
			{_, _, B} when (is_atom(B) andalso is_map_key(B, L) =:= false) -> throw(variable_not_found);
			{add, A, B} -> eval_helper(A, L)+eval_helper(B, L);
			{sub, A, B} -> eval_helper(A, L)-eval_helper(B, L);
			{mul, A, B} -> eval_helper(A, L)*eval_helper(B, L);
			{'div', A, B} -> eval_helper(A, L)/eval_helper(B, L);
			A when is_atom(A) -> maps:get(A,L)
		end
	catch
		variable_not_found -> throw(variable_not_found);
		_:_ -> throw(error)
	end.

% Problem 3

map(F, L) -> map(F, L, []).
map(_F, [], Acc) -> lists:reverse(Acc);
map(F, [H|T], Acc) -> map(F, T, [F(H)|Acc]).

filter(P, L) -> filter(P, L, []).
filter(_P, [], Acc) -> lists:reverse(Acc);
filter(P, [H|T], Acc) ->
	case P(H) of
		true -> filter(P, T, [H|Acc]);
		false -> filter(P, T, Acc)
	end.

split(P, L) -> split(P, L, {[],[]}).
split(_P, [], {True, False}) -> {lists:reverse(True), lists:reverse(False)};
split(P, [H|T], {True, False}) ->
	case P(H) of
		true -> split(P, T, {[H|True], False});
		false -> split(P, T, {True, [H|False]})
	end.

groupby(F, L) -> groupby(F, L, #{}, 1).
groupby(_F, [], M, _I) -> 
	Rev = fun(K,V,Map) -> Map#{K => lists:reverse(V)} end,
  	maps:fold(Rev,#{},M);
groupby(F, [H|T], M, I)  -> 
	case is_map_key(F(H), M) of
		true -> groupby(F, T, M#{F(H) := [I|maps:get(F(H), M)]}, I+1);
		false -> groupby(F, T, M#{F(H) => [I]}, I+1)
	end.
