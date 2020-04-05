%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%   This is for consumer
%%% @end
%%% Created : 18 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ensq_channel).
-include("ensq.hrl").

%% API
-export([open/5, ready/2, close/1]).

%% gen_server callbacks
-export([init/7]).

-define(SERVER, ?MODULE).

-define(RECHECK_INTERVAL, 100).

-record(state, {
        socket,
        buffer,
        current_ready_count = 1,
        ready_count = 1,
        handler = ensq_debug_callback,
        workers
    }).

%%%===================================================================
%%% API
%%%===================================================================

open(Host, Port, Topic, Channel, Handler) ->
    Ref = make_ref(),
    Pid = spawn_link(ensq_channel, init, [self(), Ref, Host, Port, Topic, Channel, Handler]),
    receive
        {Ref, ok} -> {ok, Pid};
        {Ref, E} -> E
    after 1000 ->
        close(Pid),
        {error, timeout}
    end.

ready(Pid, N) ->
    Pid ! {ready, N}.

close(Pid) ->
    Pid ! close.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(From, Ref, Host, Port, Topic, Channel, Handler) ->
    Opts = [{active, false}, binary, {deliver, term}, {packet, raw}],
    case gen_tcp:connect(Host, Port, Opts) of
        {ok, S} ->
            logger:debug("[channel|~p:~p] connected.~n", [Host, Port]),

            logger:debug("[channel|~p:~p] Sending version.~n", [Host, Port]),
            gen_tcp:send(S, ensq_proto:encode(version)),

            logger:debug("[channel|~p:~p] Subscribing to ~s/~s.~n",
                        [Host, Port, Topic, Channel]),
            gen_tcp:send(S, ensq_proto:encode({subscribe, Topic, Channel})),

            logger:debug("[channel|~p:~p] Waiting for ack.~n", [Host, Port]),
            {ok, <<0,0,0,6,0,0,0,0,79,75>>} = gen_tcp:recv(S, 0),
            logger:debug("[channel|~p:~p] Got ack changing to active mode!~n", [Host, Port]),
            Workers = [spawn_link(fun() -> worker_loop(S, Handler) end) || _ <- lists:seq(1, 8)],
            inet:setopts(S, [{active, true}]),

            logger:debug("[channel|~p:~p] Setting Ready state to 1.~n", [Host, Port]),
            gen_tcp:send(S, ensq_proto:encode({ready, 1})),

            logger:debug("[~p:~p] Done initializing.~n", [Host, Port]),
            From ! {Ref, ok},
            case is_atom(Handler) of
                true -> ok = Handler:init();
                false -> Handler(init, S)
            end,
            {ok, Max} = application:get_env(max_in_flight),
            gen_tcp:send(S, ensq_proto:encode({ready, Max})),
            State = #state{socket = S, buffer = <<>>, handler = Handler, workers = Workers, ready_count = Max, current_ready_count = Max},
            % ensq_in_flow_manager:getrc(),
            loop(State);
        E ->
            logger:error("[channel|~s:~p] Error: ~p~n", [Host, Port, E]),
            From ! {Ref, E}
    end.

loop(State) ->
    receive
        close ->
            gen_tcp:send(State#state.socket, ensq_proto:encode(close)),
            terminate(State);
        {ready, 0} ->
            erlang:send_after(?RECHECK_INTERVAL, self(), ready_rc),
            loop(State#state{ready_count = 0, current_ready_count = 0});
        {ready, N} ->
            gen_tcp:send(State#state.socket, ensq_proto:encode({ready, N})),
            loop(State#state{ready_count = N, current_ready_count = N});
        ready_rc ->
            ensq_in_flow_manager:getrc(),
            loop(State);
        {tcp, S, Data} ->
            #state{socket = S, buffer = B, ready_count = RC, current_ready_count = CRC} = State,
            State1 = data(<<B/binary, Data/binary>>, CRC, State),
            State2 = case State1#state.current_ready_count of
                N when N < (RC / 4) ->
                    %% We don't want to ask for a propper new RC every time
                    %% this keeps the load of the flow manager by guessing
                    %% we'll get the same value back anyway.
                    % case rand:uniform(10) of
                    %     10 -> ensq_in_flow_manager:getrc();
                    %     _ -> ok
                    % end,
                    gen_tcp:send(S, ensq_proto:encode({ready, RC})),
                    State1#state{current_ready_count = RC, ready_count=RC};
                _ ->
                    State1
             end,
            loop(State2);
        {tcp_closed, S} when S =:= State#state.socket ->
            terminate(State);
        {tcp_error, S, Reason} when S =:= State#state.socket ->
            logger:error("tcp error reason: ~p~n", [Reason]),
            terminate(State);
        Info ->
            logger:warning("Unknown message: ~p~n", [Info]),
            loop(State)
    end.

terminate(_State = #state{socket = S}) ->
    gen_tcp:close(S).

%%%===================================================================
%%% Internal functions
%%%===================================================================

data(<<Size:32/integer, Raw:Size/binary, Rest/binary>>, RC, #state{workers = [W|WT]} = State) ->
    W ! Raw,
    data(Rest, RC - 1, State#state{buffer = Rest, workers = WT ++ [W]});
data(Rest, RC, State) ->
    State#state{buffer = Rest, current_ready_count = RC}.

worker_loop(S, C) ->
    receive
        Frame ->
            catch handle_frame(Frame, S, C),
            worker_loop(S, C)
    end.

handle_frame(<<?FRAME_TYPE_MESSAGE:32/integer, _Timestamp:64/integer, _Attempt:16/integer, MsgID:16/binary, Msg/binary>>,
             S, C) ->
    R = case is_atom(C) of
        true -> C:message(Msg, {S, MsgID});
        false -> C(Msg, {S, MsgID})
    end,
    Reply = case R of
        ok -> ensq_proto:encode({finish, MsgID});
        {requeue, Timeout} -> ensq_proto:encode({requeue, MsgID, Timeout})
    end,
    gen_tcp:send(S, Reply),
    ok;
handle_frame(<<?FRAME_TYPE_RESPONSE:32/integer, "_heartbeat_">>, S, _C) ->
    gen_tcp:send(S, ensq_proto:encode(nop)),
    ok;
handle_frame(<<?FRAME_TYPE_RESPONSE:32/integer, Msg/binary>>, _S, C) ->
    case is_atom(C) of
        true -> ok = C:response(ensq_proto:decode(Msg));
        false -> ok
    end,
    ok;
handle_frame(<<?FRAME_TYPE_ERROR:32/integer, _Timestamp:64/integer, _Attempt:16/integer, MsgID:16/binary, Msg/binary>>, S, C) ->
    case is_atom(C) of
        true ->
            case C:error(Msg) of
                ok ->
                    gen_tcp:send(S, ensq_proto:encode({finish, MsgID}));
                Other ->
                    logger:warning("[channel|~p] ~p -> Not finishing ~s", [Other, C, MsgID]),
                    ok
            end;
        false ->
            ok
    end;
handle_frame(Msg, _S, C) ->
    logger:warning("[channel|~p] Unknown message ~p.", [C, Msg]),
    ok.

