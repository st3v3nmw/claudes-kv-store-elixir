defmodule Server.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Server.KVStore,
      {Bandit, plug: Server.Router, port: 8080}
    ]

    opts = [strategy: :one_for_one, name: Server.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
