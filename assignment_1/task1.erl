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
-export([eval/1, eval/2]).

eval({ok, A}) when is_number(A) -> A;
% eval({ok, T}) -> 
% 	case T of
% 		{add, A, B} when (is_number(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso (is_number(B) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) -> eval({ok, A})+eval({ok, B});
% 		{mul, A, B} when (is_number(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso (is_number(B) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) -> eval({ok, A})*eval({ok, B});
% 		{sub, A, B} when (is_number(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso (is_number(B) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) -> eval({ok, A})-eval({ok, B});
% 		{'div', A, B} when (is_number(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso ((is_number(B) andalso B=/=0) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) -> eval({ok, A})/eval({ok, B});
% 		_ -> error
% 	end;

eval({ok, T}) -> 
		% {Op, A, B} when (is_number(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso (is_number(B) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) ->
	try
		case T of
			{add, A, B} -> eval({ok, A})+eval({ok, B});
			{sub, A, B} -> eval({ok, A})-eval({ok, B});
			{mul, A, B} -> eval({ok, A})*eval({ok, B});
			{'div', A, B} -> eval({ok, A})/eval({ok, B})
		end
	catch
		_:_ -> error
	end;

eval(T) -> eval({ok, T}).



% Write a function eval/2 which is functionally equivalent to eval/1, but
% accepts as its second argument a map which maps atoms to numbers. For
% instance, the call eval({add, a, b}, #{a => 1, b => 2}) return 3 and
% the call eval({mul, {add, a, 3}, b}, #{a => 1, b => 2}) return {
% ok, 8}2
% . More generally, eval(E, L) accepts as input an expression tuple
% E of three elements {Op, E1, E2} where Op is defined in Task 1 and E1 and
% E2 is either a number, atom or an expression tuple, and an Erlang map L
% that acts as lookup table for atoms. The function returns either {ok, Value
% } or {error, Reason}, where Reason is either variable_not_found if an
% atom does not exist in the lookup table or unknown_error.
% Implement the function eval/1 in the module task1 and export it.

%if A or B in expression is_atom() then we need to be able to find it in map, otherwise give {error, variable_not_found}
eval_with_map({A, L}) when is_atom(A) -> maps:get(A,L);
eval_with_map({N, _}) when is_number(N) -> N;

% eval_with_map({E, L}) -> 
	% case {E, L} of
	% 	{_, L} when is_map(L) =:= false -> unknown_error;
	% 	{{_, A, _}, L} when (is_atom(A) andalso is_map_key(A, L) =:= false) -> variable_not_found;
	% 	{{_, _, B}, L} when (is_atom(B) andalso is_map_key(B, L) =:= false) -> variable_not_found;		
	% 	{{add, A, B}, L} when (is_number(A) orelse is_atom(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso (is_number(B) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) -> eval_with_map({A, L})+eval_with_map({B, L});
	% 	{{sub, A, B}, L} when (is_number(A) orelse is_atom(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso (is_number(B) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) -> eval_with_map({A, L})-eval_with_map({B, L});
	% 	{{mul, A, B}, L} when (is_number(A) orelse is_atom(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso (is_number(B) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) -> eval_with_map({A, L})*eval_with_map({B, L});
	% 	{{'div', A, B}, L} when (is_number(A) orelse is_atom(A) orelse (is_tuple(A) andalso tuple_size(A)=:=3)) andalso ((is_number(B) andalso B=/=0) orelse (is_tuple(B) andalso tuple_size(B)=:=3)) -> eval_with_map({A, L})/eval_with_map({B, L});
	% 	_ -> error
	% end.

eval_with_map({{_, A, _}, L}) when (is_atom(A) andalso is_map_key(A, L) =:= false) -> variable_not_found;
eval_with_map({{_, _, B}, L}) when (is_atom(B) andalso is_map_key(B, L) =:= false) -> variable_not_found;

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
		_:_ -> error
	end.

eval(E, L) -> eval_with_map({E,L}).

