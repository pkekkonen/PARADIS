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

-module(p1).
-export([eval/1]).

eval({ok, A}) when is_number(A) -> A;
eval({ok, T}) -> 
	case T of
		{add, A, B} when (is_number(A) or is_tuple(A)) and (is_number(B) or is_tuple(B)) -> eval({ok, A})+eval({ok, B});
		{mul, A, B} -> eval({ok, A})*eval({ok, B});
		{sub, A, B} -> eval({ok, A})-eval({ok, B});
		{di, A, B} when B =/= 0 -> eval({ok, A})/eval({ok, B});
		_ -> error
	end;
eval(T) -> {ok, eval({ok, T})}.
