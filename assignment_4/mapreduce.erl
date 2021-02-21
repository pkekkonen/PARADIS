-module(mapreduce).
-compile(export_all).

test() ->
    Mapper = fun (_Key, Text) ->
		     [{Word, 1} || Word <- Text]
	     end,
    Reducer = fun (Word, Counts) ->
		      [{Word, lists:sum(Counts)}]
	      end,
    mapreduce(Mapper, 2, Reducer, 10, [{a, ["hello", "world", "hello", "text"]}, {b, ["world", "a", "b", "text"]}]).
		      

mapreduce_seq(Mapper, Reducer, Input) ->       
    Mapped = [{K2,V2} || {K,V} <- Input, {K2,V2} <- Mapper(K,V)],    
    reduce_seq(Reducer, Mapped).

reduce_seq(Reduce,KVs) -> 
    [KV || {K,Vs} <- groupkeys(lists:sort(KVs)), KV <- Reduce(K,Vs)]. 

%% INPUT:  [{K1, V1}, {K1, V2}, {K2, V3}]
%% OUTPUT: [{K1, [V1, V2]}, {K2, [V3]}]
groupkeys([]) ->
    [];
groupkeys([{K, V}|Rest]) ->
    groupkeys(K, [V], Rest).

groupkeys(K, Vs, [{K, V}|Rest]) ->
    groupkeys(K, [V|Vs], Rest);
groupkeys(K, Vs, Rest) ->
    [{K, lists:reverse(Vs)}|groupkeys(Rest)].

%% INPUT: [a,b,c,d], 2
%% OUTPUT: [[a,b], [c,d]]
%% INPUT: [a, b], 2
%% OUTPUT: [[],[], [a], [b]]
partition(N, L) ->
    partition(N, L, length(L)).

partition(1, L, _) ->
    [L];
partition(N, L, Len) ->
    {Prefix, Suffix} = lists:split(Len div N, L),
    [Prefix | partition(N - 1, Suffix, Len - (Len div N))].

%% Partition tagged chunks to the same list 
%%
%% INPUT: [[{1, [{K1, V1}, {K2, V2}]}, {2, [{K3, V1}]}], [{2, [{K3, V10}]}]]
%% OUTPUT: [[{K1, V1}, {K2, V2}], [{K3,V1}, {K3, V10}]]
chunk(Data, Chunks) ->
    chunk(Data, Chunks + 1, 1).

chunk(_, Chunks, I) when Chunks=:= I ->
    [];
chunk(Data, Chunks, I) ->
    [[Value || Chunk <- Data, 
	       {MapKey, MapValue} <- Chunk, 
	       MapKey =:= I, 
	       Value <- MapValue]|chunk(Data, Chunks, I + 1)].

mapreduce(Mapper, Mappers, Reducer, Reducers, Input) ->
    Self = self(),
    Ref = make_ref(),
    Partitions = partition(Mappers, Input),

    MapperPids = [spawn_mapper(Self, Ref, Mapper, Reducer, Reducers, Part) || Part <- Partitions],
    ReducerPids = [receive
		      {reducer_pids, {Pid, Ref, RedPid}} ->
			  		RedPid
		  end || Pid <- MapperPids],

	UniqueReducerPids = [ whereis(P)||P<-lists:sort(remove_duplicates(lists:flatten(ReducerPids)))],


	%% TODO: how to know which Pids we should send done to? Is it smoother to save all the Pids in a lookup table

    %% Bring all data from the mappers togheter such that all keyvalue
    %% pairs with the same id is assigned to the same reducer
    % Chunks = chunk(MapperData, Reducers),
    % ReducerPids = [spawn_reducer(Self, Ref, Reducer, Chunk) || Chunk <- Chunks],

    % TODO: här skicka meddelanden till alla reducer pids och säg att vi är done så de kan processera datan de har fått
    [Pid ! {done, Reducers} || Pid <- UniqueReducerPids],



	Output = get_data(UniqueReducerPids, [], Ref),
    % Output = [receive 
		  % {reduce, {Pid, Ref, Data}} ->
		  %     Data
	   %    end || Pid <- UniqueReducerPids],

    %% Flatten the output from the reducers
    %% sort becouse it looks nice :-)
    lists:sort(lists:flatten(Output)).

 get_data([], Res, _Ref) -> Res;
 get_data([Pid|Pids], Res, Ref) ->

 	receive 
		  {reduce, {Pid, Ref, Data}} ->
		      get_data(Pids, Res++Data, Ref)
	end.


%% INPUT: [{DataKey, [DataValue1, ..., DataValueN]}]
spawn_mapper(Master, Ref, Mapper, Reducer, Reducers, Data) ->
    spawn_link(fun () ->
		       %% phash: hash erlang term to `Reducers` bins.
		       %% In this case we tag each value with the same
		       %% value so that they will be processed by the
		       %% same reducer.
		       %TODO: skicks istället här i
		       Map = [{erlang:phash(MapKey, Reducers), {MapKey, MapValue}} ||
				 %% For each element in Data
				 {DataKey, DataValue} <- Data, 
				 %% Apply the Mapper to the key and value
				 %% and iterate over those
				 {MapKey, MapValue} <- Mapper(DataKey, DataValue)],

		       % %% Use `groupkeys` to group each tagged tupe together
		       % Master ! {map, {self(), Ref, groupkeys(lists:sort(Map))}}
		       %ska skicka till reducer, kan registrera Pid som hashkod
		       

		       lists:foreach(fun({Hashcode, {MapKey, MapValue}}) ->
				 	Reference = list_to_atom(integer_to_list(Hashcode)),

				   case whereis(Reference) of
				   		undefined ->
				   			%skapa process, registrera 
				   			ReducerPid = spawn_reducer(Master, Ref, Reducer),
				   			register(Reference, ReducerPid),
				   			Reference ! {data, [{Hashcode, {MapKey, MapValue}}]};
				   		_ ->
				   			%skicka till den piden
				   			Reference ! {data, [{Hashcode, {MapKey, MapValue}}]}
				   	end
		       	end, Map),

		     		       	Master ! {reducer_pids, {self(), Ref, [list_to_atom(integer_to_list(Hashcode)) || {Hashcode, {_MapKey, _MapValue}}<-Map]}}



	       end).


remove_duplicates(L) -> sets:to_list(sets:from_list(L)).

    
%reduceraren kan inte börja arbeta fören den fått alla nycklar
spawn_reducer(Master, Ref, Reducer) ->
    spawn_link(fun () ->
    			Chunk = reducer_loop([]),
    			% TODO: se till att den verkligen stannar här och väntar tills loopen är klar


		       %% Group the values for each key and apply
		       %% The reducer to each K, value list pair
		       Reduce = [KV || {K,Vs} <- groupkeys(lists:sort(lists:flatten(Chunk))),
				       KV <- Reducer(K,Vs)],

		       Master ! {reduce, {self(), Ref, Reduce}}

	       end).




reducer_loop(Data) ->
	receive
		{data, IncomingData} ->
			reducer_loop(Data ++ IncomingData);
		{done, Reducers} ->
			chunk([groupkeys(lists:sort(lists:flatten(Data)))], Reducers)
	end.

























% -module(mapreduce).
% -compile(export_all).

% test() ->
%     Mapper = fun (_Key, Text) ->
% 		     [{Word, 1} || Word <- Text]
% 	     end,
%     Reducer = fun (Word, Counts) ->
% 		      [{Word, lists:sum(Counts)}]
% 	      end,
%     mapreduce(Mapper, 2, Reducer, 10, [{a, ["hello", "world", "hello", "text"]}, {b, ["world", "a", "b", "text"]}]).
		      

% mapreduce_seq(Mapper, Reducer, Input) ->       
%     Mapped = [{K2,V2} || {K,V} <- Input, {K2,V2} <- Mapper(K,V)],    
%     reduce_seq(Reducer, Mapped).

% reduce_seq(Reduce,KVs) -> 
%     [KV || {K,Vs} <- groupkeys(lists:sort(KVs)), KV <- Reduce(K,Vs)]. 

% %% INPUT:  [{K1, V1}, {K1, V2}, {K2, V3}]
% %% OUTPUT: [{K1, [V1, V2]}, {K2, [V3]}]
% groupkeys([]) ->
%     [];
% groupkeys([{K, V}|Rest]) ->
%     groupkeys(K, [V], Rest).

% groupkeys(K, Vs, [{K, V}|Rest]) ->
%     groupkeys(K, [V|Vs], Rest);
% groupkeys(K, Vs, Rest) ->
%     [{K, lists:reverse(Vs)}|groupkeys(Rest)].

% %% INPUT: [a,b,c,d], 2
% %% OUTPUT: [[a,b], [c,d]]
% %% INPUT: [a, b], 2
% %% OUTPUT: [[],[], [a], [b]]
% partition(N, L) ->
%     partition(N, L, length(L)).

% partition(1, L, _) ->
%     [L];
% partition(N, L, Len) ->
%     {Prefix, Suffix} = lists:split(Len div N, L),
%     [Prefix | partition(N - 1, Suffix, Len - (Len div N))].

% %% Partition tagged chunks to the same list 
% %%
% %% INPUT: [[{1, [{K1, V1}, {K2, V2}]}, {2, [{K3, V1}]}], [{2, [{K3, V10}]}]]
% %% OUTPUT: [[{K1, V1}, {K2, V2}], [{K3,V1}, {K3, V10}]]
% chunk(Data, Chunks) ->
%     chunk(Data, Chunks + 1, 1).

% chunk(_, Chunks, I) when Chunks=:= I ->
%     [];
% chunk(Data, Chunks, I) ->
%     [[Value || Chunk <- Data, 
% 	       {MapKey, MapValue} <- Chunk, 
% 	       MapKey =:= I, 
% 	       Value <- MapValue]|chunk(Data, Chunks, I + 1)].

% mapreduce(Mapper, Mappers, Reducer, Reducers, Input) ->
%     Self = self(),
%     Ref = make_ref(),
%     Partitions = partition(Mappers, Input),

%     MapperPids = [spawn_mapper(Self, Ref, Mapper, Reducers, Part) || Part <- Partitions],
%     MapperData = [receive
% 		      {map, {Pid, Ref, Data}} ->
% 			  Data
% 		  end || Pid <- MapperPids],

% 	io:format("MapperData: ~p ~n~n", [MapperData]),

%     %% Bring all data from the mappers togheter such that all keyvalue
%     %% pairs with the same id is assigned to the same reducer
%     Chunks = chunk(MapperData, Reducers),
% 	io:format("CHUNKS: ~p ~n~n", [Chunks]),

%     ReducerPids = [spawn_reducer(Self, Ref, Reducer, Chunk) || Chunk <- Chunks],
%     Output = [receive
% 		  {reduce, {Pid, Ref, Data}} ->
% 		      Data
% 	      end || Pid <- ReducerPids],

%     %% Flatten the output from the reducers
%     %% sort becouse it looks nice :-)
%     lists:sort(lists:flatten(Output)).

% %% INPUT: [{DataKey, [DataValue1, ..., DataValueN]}]
% spawn_mapper(Master, Ref, Mapper, Reducers, Data) ->
%     spawn_link(fun () ->
% 		       %% phash: hash erlang term to `Reducers` bins.
% 		       %% In this case we tag each value with the same
% 		       %% value so that they will be processed by the
% 		       %% same reducer.
% 		       Map = [{erlang:phash(MapKey, Reducers), {MapKey, MapValue}} ||
% 				 %% For each element in Data
% 				 {DataKey, DataValue} <- Data, 
% 				 %% Apply the Mapper to the key and value
% 				 %% and iterate over those
% 				 {MapKey, MapValue} <- Mapper(DataKey, DataValue)],
% 				 % io:format("~f  ~n", [lists:flatten(Map)]),

% 				io:format("Map: ~p ~n~n", [Map]),

% 		       %% Use `groupkeys` to group each tagged tupe together
% 		       Master ! {map, {self(), Ref, groupkeys(lists:sort(Map))}}
% 	       end).
    
% spawn_reducer(Master, Ref, Reducer, Chunk) ->
%     spawn_link(fun () ->
% 		       %% Group the values for each key and apply
% 		       %% The reducer to each K, value list pair
% 				io:format("CHUNK HERE: Chunk ~p ~n", [lists:flatten(Chunk)]),

% 		       Reduce = [KV || {K,Vs} <- groupkeys(lists:sort(Chunk)),
% 				       KV <- Reducer(K,Vs)],
% 		       Master ! {reduce, {self(), Ref, Reduce}}
% 	       end).
		       		       
    

