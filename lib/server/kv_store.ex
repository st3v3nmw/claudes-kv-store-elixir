defmodule Server.KVStore do
  use GenServer
  require Logger

  @store_file "store.json"

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def put(key, value), do: GenServer.call(__MODULE__, {:put, key, value})
  def get(key), do: GenServer.call(__MODULE__, {:get, key})
  def delete(key), do: GenServer.call(__MODULE__, {:delete, key})
  def clear(), do: GenServer.call(__MODULE__, :clear)

  @impl true
  def init(_) do
    # Trap exits so terminate/2 is called when the supervisor shuts us down
    Process.flag(:trap_exit, true)
    state = load_from_disk()
    {:ok, state}
  end

  @impl true
  def handle_call({:put, key, value}, _from, state) do
    {:reply, :ok, Map.put(state, key, value)}
  end

  def handle_call({:get, key}, _from, state) do
    {:reply, Map.fetch(state, key), state}
  end

  def handle_call({:delete, key}, _from, state) do
    {:reply, :ok, Map.delete(state, key)}
  end

  def handle_call(:clear, _from, _state) do
    {:reply, :ok, %{}}
  end

  @impl true
  def terminate(_reason, state) do
    flush_to_disk(state)
  end

  defp data_dir, do: System.get_env("DATA_DIR", "/app/data")

  defp store_path, do: Path.join(data_dir(), @store_file)

  defp load_from_disk do
    path = store_path()

    with true <- File.exists?(path),
         {:ok, contents} <- File.read(path),
         {:ok, map} when is_map(map) <- Jason.decode(contents) do
      Logger.info("Restored #{map_size(map)} keys from #{path}")
      map
    else
      false ->
        Logger.info("No store file found at #{path}, starting empty")
        %{}

      {:error, reason} ->
        Logger.warning("Could not load store (#{inspect(reason)}), starting empty")
        %{}
    end
  end

  defp flush_to_disk(state) do
    path = store_path()
    File.mkdir_p!(Path.dirname(path))

    tmp = path <> ".tmp"
    File.write!(tmp, Jason.encode!(state))
    File.rename!(tmp, path)

    Logger.info("Flushed #{map_size(state)} keys to #{path}")
  end
end
