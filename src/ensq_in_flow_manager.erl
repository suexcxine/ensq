%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 19 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ensq_in_flow_manager).
-behaviour(gen_server).

%% API
-export([start_link/0, getrc/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
        max_in_flight,
        channels = [],
        last_count = 0
    }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

getrc() ->
    gen_server:cast(?SERVER, {getrc, self()}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, Max} = application:get_env(max_in_flight),
    {ok, #state{max_in_flight = Max}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({getrc, From}, State = #state{channels = Cs, max_in_flight = Max}) ->
    case lists:keyfind(From, 2, Cs) of
        false ->
            InUse = lists:sum([N || {N, _, _} <- Cs]),
            Free = Max - InUse,
            {N, CsN} =
                case length(Cs) of
                    %% We have more channels then we have max in flight
                    %% so this one will have to wait.
                    L when L >= Max ->
                        {0, Cs};
                    %% This is the first channel we hand out 50%
                    0 ->
                        {erlang:trunc(Max * 0.5), Cs};
                    %% If we have more then the average free we can just go and use that
                    L when Free > Max/L ->
                        Avg = erlang:trunc(Max * 0.5/L),
                        {Avg, Cs};
                    %% We have only a few channels so we might be able to
                    %% grab a few from the largest channel.
                    L when L < (Max / 20) ->
                        [{NMax, PidMax, RefMax} | CsS] = lists:sort(Cs),
                        N1 = erlang:trunc(NMax / 2),
                        ensq_channel:ready(PidMax, N1),
                        {N1, [{NMax, PidMax, RefMax} | CsS]};
                    %% We'll have to reshuffle the ready count
                    %% Every channel will get a equal share of 75%
                    L ->
                        RC = erlang:trunc((Max / L) * 0.75),
                        Cs1 = [{RC, P, R} || {_, P, R} <- Cs],
                        [ensq_channel:ready(Pid, N) || {N, Pid, _} <- Cs1],
                        {RC, Cs1}
                end,
            Ref = erlang:monitor(process, From),
            ensq_channel:ready(From, N),
            {noreply, State#state{channels=[{N, From, Ref}|CsN]}};
        %% We got a receive count!
        {N, From, Ref} ->
            %% If we only have one channel we slowsly scale this up
            case length(Cs) of
                1 ->
                    RC1 = erlang:min(Max * 0.9, N * 1.1),
                    ensq_channel:ready(From, erlang:trunc(RC1)),
                    {noreply, State};
                L ->
                    Avg = Max / L,
                    InUse = lists:sum([Nu || {Nu, _, _} <- Cs]),
                    Free = Max - InUse,
                    NewN = erlang:trunc(erlang:min(Free, Avg)),
                    CsN = lists:keydelete(From, 2, Cs),
                    ensq_channel:ready(From, NewN),
                    {noreply, State#state{channels=[{NewN, From, Ref}|CsN]}}
            end
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, _, _, _}, State = #state{channels=Cs}) ->
    {noreply, State#state{channels = lists:keydelete(Ref, 3, Cs)}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Inner Functions
%%%===================================================================

