% Write a function eval/1 which takes as input a tuple and evaluates the
% mathematical expression it denotes. For instance, the call eval({add, 1,
% 1}) would return {ok, 2}, and the call eval({mul, {add, 2, 2}, 4})
% would return {ok, 16}1
% . More generally, the function accepts as input an
% expression tuple E of three (3) elements {Op, E1, E2}, where Op is add,
% mul, div or sub and E1 and E2 are either numbers or expression tuples (see
% example), and return the answer as the tuple {ok, Value}, or the atom
% error if the evaluation fails for any reason.
% Implement the function eval/1 in the module task1 and export it.

-module(task1).
-export([eval/1, eval/2, map/2, filter/2, split/2, groupby/2]).


% eval/1
eval_second(A) when is_number(A) -> A;
eval_second(T) -> 
		% {Op, A, B} when (is_number(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso (is_number(B) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) ->
	try
		case T of
			{add, A, B} -> eval_second(A)+eval_second(B);
			{sub, A, B} -> eval_second(A)-eval_second(B);
			{mul, A, B} -> eval_second(A)*eval_second(B);
			{'div', A, B} -> eval_second(A)/eval_second(B)
		end
	catch
		_:_ -> error
	end.

eval(T) ->
	try
		case T of
			{add, A, B} -> {ok, eval_second(A)+eval_second(B)};
			{sub, A, B} -> {ok, eval_second(A)-eval_second(B)};
			{mul, A, B} -> {ok, eval_second(A)*eval_second(B)};
			{'div', A, B} -> {ok, eval_second(A)/eval_second(B)}
		end
	catch
		_:_ -> error
	end.


% eval/2

eval_with_map({A, L}) when is_atom(A) -> maps:get(A,L);
eval_with_map({N, _}) when is_number(N) -> N;
eval_with_map({{_, A, _}, L}) when (is_atom(A) andalso is_map_key(A, L) =:= false) -> {error, variable_not_found};
eval_with_map({{_, _, B}, L}) when (is_atom(B) andalso is_map_key(B, L) =:= false) -> {error, variable_not_found};
eval_with_map({E, L}) -> 
	try
		case {E, L} of
			% {{_, A, _}, L} when (is_atom(A) andalso is_map_key(A, L) =:= false) -> throw(variable_not_found);
			% {{_, _, B}, L} when (is_atom(B) andalso is_map_key(B, L) =:= false) -> throw(variable_not_found);	
			{{add, A, B}, L} -> eval_with_map({A, L})+eval_with_map({B, L});
			{{sub, A, B}, L} -> eval_with_map({A, L})-eval_with_map({B, L});
			{{mul, A, B}, L} -> eval_with_map({A, L})*eval_with_map({B, L});
			{{'div', A, B}, L} -> eval_with_map({A, L})/eval_with_map({B, L})
		end
	catch
		_:_ -> {error, unknown_error}
	end.

eval({_, A, _}, L) when (is_atom(A) andalso is_map_key(A, L) =:= false) -> {error, variable_not_found};
eval({_, _, B}, L) when (is_atom(B) andalso is_map_key(B, L) =:= false) -> {error, variable_not_found};
eval(E, L) -> 
	try
		% TODO: kolla om kan köra bara case E
		case {E, L} of
			% {{_, A, _}, L} when (is_atom(A) andalso is_map_key(A, L) =:= false) -> throw(variable_not_found);
			% {{_, _, B}, L} when (is_atom(B) andalso is_map_key(B, L) =:= false) -> throw(variable_not_found);	
			{{add, A, B}, L} -> {ok, eval_with_map({A, L})+eval_with_map({B, L})};
			{{sub, A, B}, L} -> {ok, eval_with_map({A, L})-eval_with_map({B, L})};
			{{mul, A, B}, L} -> {ok, eval_with_map({A, L})*eval_with_map({B, L})};
			{{'div', A, B}, L} -> {ok, eval_with_map({A, L})/eval_with_map({B, L})}
		end
	catch
		_:_ -> {error, unknown_error}
	end.





% Implement the higer-order functions in Table using tail recursion but without using 
% list-comperhensions or the lists-module3. Ensure that the functions preserve the order of elements.

% Function Definition
% map(F, L) Return a new list which is the result of applying the
% function F to every element in L. 


map(F, L) -> map(F, L, []).
map(_F, [], Acc) -> lists:reverse(Acc);
map(F, [H|T], Acc) -> map(F, T, [F(H)|Acc]).

% filter(P, L) Return a new list which is the result of filtering out
% the elements in L for which the function P returns true.

filter(P, L) -> filter(P, L, []).
filter(_P, [], Acc) -> lists:reverse(Acc);
filter(P, [H|T], Acc) ->
	case P(H) of
		true -> filter(P, T, [H|Acc]);
		false -> filter(P, T, Acc)
	end.

% split(P, L) Return a tuple with two lists, {True, False} where
% True is a list containing the elements of L for which P
% returns true and False is a list containing the elements
% of L for which P returns false.

split(P, L) -> split(P, L, {[],[]}).
split(_P, [], {True, False}) -> {lists:reverse(True), lists:reverse(False)};
split(P, [H|T], {True, False}) ->
	case P(H) of
		true -> split(P, T, {[H|True], False});
		false -> split(P, T, {True, [H|False]})
	end.

% groupby(F, L) Return a map with #{K1 => I1, ..., Kp => Ip}
% where Ii is a lists of indices to the values in L where
% F returns Ki (see example). Awarded a maximum of 4
% points

groupby(F, L) -> groupby(F, L, #{}, 1).
groupby(_F, [], M, _I) -> 
	Rev = fun(K,V,Map) -> Map#{K => lists:reverse(V)} end,
  	maps:fold(Rev,#{},M);
%{lists:reverse(True), lists:reverse(False)};
groupby(F, [H|T], M, I)  -> 
	case is_map_key(F(H), M) of
		true -> groupby(F, T, M#{F(H) := [I|maps:get(F(H), M)]}, I+1);
		false -> groupby(F, T, M#{F(H) => [I]}, I+1)
	end.


% 	task1:groupby(fun (X) -> if X < 0 -> negative;
% X > 0 -> positive;
% true -> zero
% end
% end, [-1, 11, 10, -4, 0]).































