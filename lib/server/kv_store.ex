defmodule Server.KVStore do
  use GenServer
  require Logger

  @snapshot_file "snapshot.json"
  @wal_file "wal.jsonl"
  # Checkpoint (snapshot + truncate WAL) every N write operations
  @checkpoint_interval 500

  # ── Public API ────────────────────────────────────────────────────────────

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def put(key, value), do: GenServer.call(__MODULE__, {:put, key, value})
  def get(key), do: GenServer.call(__MODULE__, {:get, key})
  def delete(key), do: GenServer.call(__MODULE__, {:delete, key})
  def clear(), do: GenServer.call(__MODULE__, :clear)

  # ── Callbacks ─────────────────────────────────────────────────────────────

  @impl true
  def init(_) do
    Process.flag(:trap_exit, true)

    dir = data_dir()
    File.mkdir_p!(dir)

    data = load_snapshot(dir) |> replay_wal(dir)
    wal_fd = open_wal(dir)

    Logger.info("KVStore ready with #{map_size(data)} keys")
    {:ok, %{data: data, wal_fd: wal_fd, op_count: 0}}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    state = append_and_sync(state, %{op: "set", k: key, v: value})
    state = %{state | data: Map.put(state.data, key, value)}
    {:reply, :ok, maybe_checkpoint(state)}
  end

  def handle_call({:get, key}, _from, state) do
    {:reply, Map.fetch(state.data, key), state}
  end

  def handle_call({:delete, key}, _from, state) do
    state = append_and_sync(state, %{op: "del", k: key})
    state = %{state | data: Map.delete(state.data, key)}
    {:reply, :ok, maybe_checkpoint(state)}
  end

  def handle_call(:clear, _from, state) do
    state = append_and_sync(state, %{op: "clr"})
    state = %{state | data: %{}}
    {:reply, :ok, maybe_checkpoint(state)}
  end

  @impl true
  def terminate(_reason, state) do
    # On graceful shutdown, snapshot current state and close the WAL.
    write_snapshot(state.data)
    :file.close(state.wal_fd)
    truncate_wal()
    Logger.info("KVStore flushed #{map_size(state.data)} keys on shutdown")
  end

  # ── WAL helpers ───────────────────────────────────────────────────────────

  defp append_and_sync(state, entry) do
    line = Jason.encode!(entry) <> "\n"
    :ok = :file.write(state.wal_fd, line)
    :ok = :file.sync(state.wal_fd)
    %{state | op_count: state.op_count + 1}
  end

  defp open_wal(dir) do
    path = wal_path(dir)
    {:ok, fd} = :file.open(String.to_charlist(path), [:append, :raw, :binary])
    fd
  end

  defp maybe_checkpoint(%{op_count: count} = state) when count >= @checkpoint_interval do
    Logger.info("Checkpointing at #{count} ops")
    write_snapshot(state.data)
    :file.close(state.wal_fd)
    truncate_wal()
    new_fd = open_wal(data_dir())
    %{state | wal_fd: new_fd, op_count: 0}
  end

  defp maybe_checkpoint(state), do: state

  # ── Snapshot helpers ──────────────────────────────────────────────────────

  defp write_snapshot(data) do
    path = snapshot_path(data_dir())
    tmp = path <> ".tmp"
    File.write!(tmp, Jason.encode!(data))
    File.rename!(tmp, path)
  end

  defp truncate_wal do
    File.write!(wal_path(data_dir()), "")
  end

  defp load_snapshot(dir) do
    path = snapshot_path(dir)

    with true <- File.exists?(path),
         {:ok, contents} <- File.read(path),
         {:ok, map} when is_map(map) <- Jason.decode(contents) do
      Logger.info("Loaded snapshot: #{map_size(map)} keys")
      map
    else
      _ -> %{}
    end
  end

  defp replay_wal(data, dir) do
    path = wal_path(dir)

    if File.exists?(path) do
      entries =
        path
        |> File.stream!()
        |> Enum.reduce(data, fn line, acc ->
          line = String.trim(line)

          if line == "" do
            acc
          else
            case Jason.decode(line) do
              {:ok, %{"op" => "set", "k" => k, "v" => v}} -> Map.put(acc, k, v)
              {:ok, %{"op" => "del", "k" => k}} -> Map.delete(acc, k)
              {:ok, %{"op" => "clr"}} -> %{}
              _ -> acc
            end
          end
        end)

      Logger.info("Replayed WAL, now #{map_size(entries)} keys")
      entries
    else
      data
    end
  end

  # ── Paths ─────────────────────────────────────────────────────────────────

  defp data_dir, do: System.get_env("DATA_DIR", "/app/data")
  defp snapshot_path(dir), do: Path.join(dir, @snapshot_file)
  defp wal_path(dir), do: Path.join(dir, @wal_file)
end
