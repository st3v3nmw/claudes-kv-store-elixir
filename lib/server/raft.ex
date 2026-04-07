defmodule Server.Raft do
  use GenServer
  require Logger

  @election_timeout_min 500
  @election_timeout_max 1000
  @heartbeat_interval 100
  @rpc_timeout 250
  @raft_meta_file "raft_meta.json"

  # A follower considers its leader "known" only while heartbeats arrive regularly.
  # After this many ms without a heartbeat, the leader is treated as unreachable.
  @leader_stale_ms 250

  defstruct id: nil,
            peers: [],
            current_term: 0,
            voted_for: nil,
            role: :follower,
            leader_id: nil,
            last_heartbeat_ms: nil,
            votes_received: MapSet.new(),
            # peer_id => monotonic_ms of last successful AppendEntries response.
            # Used by leaders to detect when they lose quorum and must step down.
            peer_acks: %{},
            election_timer_ref: nil

  # ── Public API ────────────────────────────────────────────────────────────

  def start_link(_), do: GenServer.start_link(__MODULE__, [], name: __MODULE__)

  def get_state, do: GenServer.call(__MODULE__, :get_state)
  def handle_request_vote(params), do: GenServer.call(__MODULE__, {:request_vote, params})
  def handle_append_entries(params), do: GenServer.call(__MODULE__, {:append_entries, params})

  @doc "Returns true if this node has a leader that is still sending heartbeats."
  def leader_known?(%{role: :leader}), do: true
  def leader_known?(%{leader_id: nil}), do: false
  def leader_known?(%{last_heartbeat_ms: nil}), do: false

  def leader_known?(%{last_heartbeat_ms: t}) do
    System.monotonic_time(:millisecond) - t < @leader_stale_ms
  end

  # ── Init ──────────────────────────────────────────────────────────────────

  @impl true
  def init(_) do
    id = System.get_env("ADDR", "localhost:8080")

    peers =
      System.get_env("PEERS", "")
      |> String.split(",")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    {current_term, voted_for} = load_persistent_state()

    state = %__MODULE__{
      id: id,
      peers: peers,
      current_term: current_term,
      voted_for: voted_for
    }

    {:ok, reset_election_timer(state)}
  end

  # ── handle_info ───────────────────────────────────────────────────────────

  @impl true
  def handle_info(:election_timeout, state) do
    Logger.info("[Raft] #{state.id} starting election for term #{state.current_term + 1}")
    {:noreply, start_election(state)}
  end

  def handle_info(:send_heartbeats, %{role: :leader} = state) do
    broadcast_append_entries(state)
    schedule_heartbeat()
    {:noreply, check_leader_quorum(state)}
  end

  def handle_info(:send_heartbeats, state), do: {:noreply, state}

  def handle_info({:vote_response, from_peer, from_term, vote_granted}, state) do
    {:noreply, process_vote_response(state, from_peer, from_term, vote_granted)}
  end

  def handle_info({:append_entries_response, peer, from_term, success}, state) do
    state =
      if from_term > state.current_term do
        step_down(state, from_term) |> reset_election_timer()
      else
        state
      end

    state =
      if state.role == :leader and success do
        %{state | peer_acks: Map.put(state.peer_acks, peer, System.monotonic_time(:millisecond))}
      else
        state
      end

    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  # ── handle_call ───────────────────────────────────────────────────────────

  @impl true
  def handle_call(:get_state, _from, state), do: {:reply, state, state}

  def handle_call({:request_vote, params}, _from, state) do
    {reply, state} = do_request_vote(state, params)
    {:reply, reply, state}
  end

  def handle_call({:append_entries, params}, _from, state) do
    {reply, state} = do_append_entries(state, params)
    {:reply, reply, state}
  end

  # ── Election ─────────────────────────────────────────────────────────────

  defp start_election(state) do
    new_term = state.current_term + 1
    me = self()

    state = %{state |
      role: :candidate,
      current_term: new_term,
      voted_for: state.id,
      votes_received: MapSet.new([state.id]),
      leader_id: nil
    }

    persist_state(state)
    state = reset_election_timer(state)

    # Check for immediate majority (single-node case)
    if MapSet.size(state.votes_received) >= majority_size(state) do
      become_leader(state)
    else
      for peer <- state.peers do
        Task.start(fn ->
          result =
            Req.post("http://#{peer}/raft/request-vote",
              json: %{
                "term" => new_term,
                "candidate-id" => state.id,
                "last-log-index" => 0,
                "last-log-term" => 0
              },
              receive_timeout: @rpc_timeout,
              retry: false
            )

          case result do
            {:ok, %{status: 200, body: %{"term" => t, "vote-granted" => g}}} ->
              send(me, {:vote_response, peer, t, g})

            _ ->
              :ok
          end
        end)
      end

      state
    end
  end

  defp process_vote_response(state, from_peer, from_term, vote_granted) do
    state = if from_term > state.current_term, do: step_down(state, from_term), else: state

    if state.role != :candidate or state.current_term != from_term do
      state
    else
      if vote_granted do
        votes = MapSet.put(state.votes_received, from_peer)
        state = %{state | votes_received: votes}

        if MapSet.size(votes) >= majority_size(state) do
          become_leader(state)
        else
          state
        end
      else
        state
      end
    end
  end

  defp become_leader(state) do
    Logger.info("[Raft] #{state.id} became LEADER for term #{state.current_term}")
    if state.election_timer_ref, do: Process.cancel_timer(state.election_timer_ref)

    # Seed peer_acks from the voters that just elected us.
    # This gives ~300ms grace before the quorum check starts demanding real heartbeat acks.
    now = System.monotonic_time(:millisecond)

    peer_acks =
      state.votes_received
      |> Enum.reject(&(&1 == state.id))
      |> Enum.into(%{}, &{&1, now})

    state = %{state |
      role: :leader,
      leader_id: state.id,
      election_timer_ref: nil,
      peer_acks: peer_acks
    }

    broadcast_append_entries(state)
    schedule_heartbeat()
    state
  end

  defp broadcast_append_entries(state) do
    me = self()

    for peer <- state.peers do
      Task.start(fn ->
        result =
          Req.post("http://#{peer}/raft/append-entries",
            json: %{
              "term" => state.current_term,
              "leader-id" => state.id,
              "prev-log-index" => 0,
              "prev-log-term" => 0,
              "entries" => [],
              "leader-commit" => 0
            },
            receive_timeout: @rpc_timeout,
            retry: false
          )

        case result do
          {:ok, %{status: 200, body: %{"term" => t, "success" => s}}} ->
            send(me, {:append_entries_response, peer, t, s})

          _ ->
            :ok
        end
      end)
    end
  end

  # Check whether the leader is still acknowledged by a majority of peers.
  # If not, step down to follower so the cluster can elect a new leader.
  defp check_leader_quorum(state) do
    threshold = System.monotonic_time(:millisecond) - @heartbeat_interval * 3

    fresh_peers =
      Enum.count(state.peer_acks, fn {_peer, last_ack} -> last_ack > threshold end)

    if fresh_peers + 1 >= majority_size(state) do
      state
    else
      Logger.info("[Raft] #{state.id} lost quorum (#{fresh_peers + 1}/#{majority_size(state)}), stepping down")
      state |> step_down_same_term() |> reset_election_timer()
    end
  end

  # ── RPC handlers ─────────────────────────────────────────────────────────

  defp do_request_vote(state, %{"term" => term, "candidate-id" => candidate_id}) do
    if term < state.current_term do
      {%{"term" => state.current_term, "vote-granted" => false}, state}
    else
      original_term = state.current_term
      state = if term > original_term, do: step_down(state, term), else: state

      vote_granted = state.voted_for == nil or state.voted_for == candidate_id
      state = if vote_granted, do: %{state | voted_for: candidate_id}, else: state

      if term > original_term or vote_granted, do: persist_state(state)

      state = if vote_granted, do: reset_election_timer(state), else: state

      {%{"term" => state.current_term, "vote-granted" => vote_granted}, state}
    end
  end

  defp do_append_entries(state, %{"term" => term, "leader-id" => leader_id}) do
    if term < state.current_term do
      {%{"term" => state.current_term, "success" => false}, state}
    else
      original_term = state.current_term
      state = if term > original_term, do: step_down(state, term), else: state

      state = %{state |
        role: :follower,
        leader_id: leader_id,
        last_heartbeat_ms: System.monotonic_time(:millisecond)
      }

      if term > original_term, do: persist_state(state)

      state = reset_election_timer(state)
      {%{"term" => state.current_term, "success" => true}, state}
    end
  end

  # Step down to follower, moving to a higher term (e.g. on seeing a higher-term message).
  defp step_down(state, new_term) do
    if state.election_timer_ref, do: Process.cancel_timer(state.election_timer_ref)

    %{state |
      current_term: new_term,
      voted_for: nil,
      role: :follower,
      leader_id: nil,
      votes_received: MapSet.new(),
      peer_acks: %{},
      election_timer_ref: nil
    }
  end

  # Step down while keeping the current term (e.g. on losing quorum).
  defp step_down_same_term(state) do
    if state.election_timer_ref, do: Process.cancel_timer(state.election_timer_ref)

    %{state |
      role: :follower,
      leader_id: nil,
      votes_received: MapSet.new(),
      peer_acks: %{},
      election_timer_ref: nil
    }
  end

  # ── Timers ────────────────────────────────────────────────────────────────

  defp reset_election_timer(state) do
    if state.election_timer_ref, do: Process.cancel_timer(state.election_timer_ref)

    range = @election_timeout_max - @election_timeout_min
    timeout = @election_timeout_min + :rand.uniform(range)
    ref = Process.send_after(self(), :election_timeout, timeout)
    %{state | election_timer_ref: ref}
  end

  defp schedule_heartbeat do
    Process.send_after(self(), :send_heartbeats, @heartbeat_interval)
  end

  # ── Helpers ───────────────────────────────────────────────────────────────

  defp majority_size(state) do
    total = length(state.peers) + 1
    div(total, 2) + 1
  end

  # ── Persistence ───────────────────────────────────────────────────────────

  defp persist_state(state) do
    dir = data_dir()
    File.mkdir_p!(dir)
    path = Path.join(dir, @raft_meta_file)
    tmp = path <> ".tmp"
    content = Jason.encode!(%{"current_term" => state.current_term, "voted_for" => state.voted_for})
    {:ok, fd} = :file.open(String.to_charlist(tmp), [:write, :raw, :binary])
    :file.write(fd, content)
    :file.sync(fd)
    :file.close(fd)
    File.rename!(tmp, path)
  end

  defp load_persistent_state do
    path = Path.join(data_dir(), @raft_meta_file)

    with true <- File.exists?(path),
         {:ok, contents} <- File.read(path),
         {:ok, %{"current_term" => term, "voted_for" => voted}} <- Jason.decode(contents) do
      {term, voted}
    else
      _ -> {0, nil}
    end
  end

  defp data_dir, do: System.get_env("DATA_DIR", "/app/data")
end
