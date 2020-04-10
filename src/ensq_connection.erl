%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%   This is for producer
%%% @end
%%% Created : 18 Jan 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ensq_connection).

-behaviour(gen_server).

-include("ensq.hrl").

%% API
-export([open/3, start_link/3, send/3, close/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(RECHECK_INTERVAL, 100).

-record(state, {
        socket,
        buffer,
        topic,
        from = queue:new(),
        host,
        port
    }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Host, Port, Topic) ->
    gen_server:start_link(?MODULE, [Host, Port, Topic], []).

open(Host, Port, Topic) ->
    ensq_connection_sup:start_child(Host, Port, Topic).

send(Pid, From, Topic) ->
    gen_server:cast(Pid, {send, From, Topic}).

close(Pid) ->
    gen_server:call(Pid, close).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Host, Port, Topic]) ->
    {ok, connect(#state{topic = Topic, host = Host, port = Port})}.

connect(State = #state{host = Host, port = Port}) ->
    logger:info("[~p:~p] Connecting.", [Host, Port]),
    case State#state.socket of
        undefined ->
            ok;
        Old ->
            logger:info("[~p:~p] Closing as part of connect.", [Host, Port]),
            gen_tcp:close(Old)
    end,
    Opts = [{active, true}, binary, {deliver, term}, {packet, raw}],
    State1 = State#state{socket = undefined, buffer = <<>>, from = queue:new()},
    case gen_tcp:connect(binary_to_list(Host), Port, Opts) of
        {ok, Socket} ->
            case gen_tcp:send(Socket, ensq_proto:encode(version)) of
                ok ->
                    logger:info("[~p:~p] Connected to: ~p.", [Host, Port, Socket]),
                    State1#state{socket = Socket};
                E ->
                    logger:info("[~p:~p] Connection errror: ~p.", [Host, Port, E]),
                    State1
            end;
        E ->
            logger:error("[~p:~p] target Error: ~p~n", [Host, Port, E]),
            State1
    end.

handle_call(close, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({send, From, Msg}, State=#state{socket=undefined}) ->
    handle_cast({send, From, Msg}, connect(State));

handle_cast({send, From, Msg}, State=#state{socket=S, topic=Topic, from = F}) ->
    case gen_tcp:send(S, ensq_proto:encode({publish, Topic, Msg})) of
        ok ->
            {noreply, State#state{from = queue:in(From, F)}};
        E ->
            logger:warning("[~s] Ooops: ~p~n", [Topic, E]),
            gen_server:reply(From, E),
            {noreply, connect(State)}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp, S, Data}, State=#state{socket = S, buffer = B}) ->
    State1 = data(State#state{buffer = <<B/binary, Data/binary>>}),
    {noreply, State1};

handle_info({tcp, S, Data}, State=#state{socket = S0, buffer = B}) ->
    logger:info("[~p:~p] Got data from ~p but socket should be ~p.", [State#state.host, State#state.port, S, S0]),
    State1 = data(State#state{buffer = <<B/binary, Data/binary>>, socket = S}),
    {noreply, State1#state{socket = S0}};

handle_info({tcp_closed, S}, State = #state{socket = S}) ->
    logger:info("[~p:~p] Remote side hung up.", [State#state.host, State#state.port]),
    {noreply, connect(State)};

handle_info({tcp_error, S, Reason}, State = #state{socket = S}) ->
    logger:info("[~p:~p] Tcp error, reason: ~p.", [State#state.host, State#state.port, Reason]),
    {noreply, connect(State)};

handle_info(Info, State) ->
    logger:info("[~p:~p] Unknown handle_info msg: ~p", [State#state.host, State#state.port, Info]),
    {noreply, State}.

terminate(_Reason, _State = #state{socket = S}) ->
    gen_tcp:close(S),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

data(#state{buffer = <<Size:32/integer, Raw:Size/binary, Rest/binary>>} = State) ->
    State3 =
    case handle_frame(Raw, State) of
        {state, State2} -> State2#state{buffer = Rest};
        _ -> State#state{buffer = Rest}
    end,
    data(State3);
data(State) ->
    State.

handle_frame(<<?FRAME_TYPE_RESPONSE:32/integer, "_heartbeat_">>, #state{socket = S}) ->
    gen_tcp:send(S, ensq_proto:encode(nop));
handle_frame(<<?FRAME_TYPE_RESPONSE:32/integer, Data/binary>>, #state{socket = S, from = F} = State) ->
    {{value, From}, F1} = queue:out(F),
    case ensq_proto:decode(Data) of
        #message{message_id = MsgID, message = Msg} ->
            gen_tcp:send(S, ensq_proto:encode({finish, MsgID})),
            gen_server:reply(From, Msg),
            {state, State#state{from = F1}};
        Msg ->
            gen_server:reply(From, Msg),
            {state, State#state{from = F1}}
    end;
handle_frame(<<?FRAME_TYPE_ERROR:32/integer, Data/binary>>, #state{socket = S}) ->
    case ensq_proto:decode(Data) of
        #message{message_id = MsgID, message = Msg} ->
            gen_tcp:send(S, ensq_proto:encode({finish, MsgID})),
            logger:info("[msg:~s] ~p", [MsgID, Msg]);
        Msg ->
            logger:info("[msg] ~p", [Msg])
    end;
handle_frame(<<?FRAME_TYPE_MESSAGE:32/integer, Data/binary>>, #state{socket = S, from = F} = State) ->
    {{value,From}, F1} = queue:out(F),
    case ensq_proto:decode(Data) of
        #message{message_id = MsgID, message = Msg} ->
            gen_tcp:send(S, ensq_proto:encode({finish, MsgID})),
            gen_server:reply(From, Msg),
            {state, State#state{from = F1}};
        Msg ->
            gen_server:reply(From, Msg),
            {state, State#state{from = F1}}
    end;
handle_frame(Msg, State) ->
    logger:warning("[unknown] ~p~n", [Msg]),
    {state, State}.

