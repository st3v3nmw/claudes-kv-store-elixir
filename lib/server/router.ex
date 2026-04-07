defmodule Server.Router do
  use Plug.Router

  plug :match
  plug :dispatch

  # ── Health ────────────────────────────────────────────────────────────────

  match "/health" do
    case conn.method do
      "GET" -> send_resp(conn, 200, "ok")
      _ -> send_resp(conn, 405, "method not allowed")
    end
  end

  # ── Cluster info ──────────────────────────────────────────────────────────

  match "/cluster/info" do
    case conn.method do
      "GET" ->
        state = Server.Raft.get_state()

        # Only report the leader if it is actively sending heartbeats.
        # A stale leader_id (partition, crash) must surface as null.
        reported_leader =
          if Server.Raft.leader_known?(state), do: state.leader_id, else: nil

        body =
          Jason.encode!(%{
            "id" => state.id,
            "role" => to_string(state.role),
            "term" => state.current_term,
            "leader" => reported_leader,
            "peers" => Enum.sort(state.peers)
          })

        conn
        |> put_resp_content_type("application/json")
        |> send_resp(200, body)

      _ ->
        send_resp(conn, 405, "method not allowed")
    end
  end

  # ── Raft RPCs ─────────────────────────────────────────────────────────────

  match "/raft/request-vote" do
    case conn.method do
      "POST" ->
        {:ok, raw, conn} = Plug.Conn.read_body(conn)

        case Jason.decode(raw) do
          {:ok, params} ->
            reply = Server.Raft.handle_request_vote(params)

            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, Jason.encode!(reply))

          _ ->
            send_resp(conn, 400, "invalid json")
        end

      _ ->
        send_resp(conn, 405, "method not allowed")
    end
  end

  match "/raft/append-entries" do
    case conn.method do
      "POST" ->
        {:ok, raw, conn} = Plug.Conn.read_body(conn)

        case Jason.decode(raw) do
          {:ok, params} ->
            reply = Server.Raft.handle_append_entries(params)

            conn
            |> put_resp_content_type("application/json")
            |> send_resp(200, Jason.encode!(reply))

          _ ->
            send_resp(conn, 400, "invalid json")
        end

      _ ->
        send_resp(conn, 405, "method not allowed")
    end
  end

  # ── Clear ─────────────────────────────────────────────────────────────────

  match "/clear" do
    case conn.method do
      "DELETE" ->
        with_leader(conn, fn conn ->
          Server.KVStore.clear()
          send_resp(conn, 200, "")
        end)

      _ ->
        send_resp(conn, 405, "method not allowed")
    end
  end

  # ── KV operations ─────────────────────────────────────────────────────────

  match "/kv" do
    case conn.method do
      m when m in ["GET", "PUT", "DELETE"] -> send_resp(conn, 400, "key cannot be empty")
      _ -> send_resp(conn, 405, "method not allowed")
    end
  end

  match "/kv/*glob" do
    key = Enum.join(conn.path_params["glob"], "/")

    if key == "" do
      case conn.method do
        m when m in ["GET", "PUT", "DELETE"] -> send_resp(conn, 400, "key cannot be empty")
        _ -> send_resp(conn, 405, "method not allowed")
      end
    else
      case conn.method do
        "PUT" -> with_leader(conn, &handle_put(&1, key))
        "GET" -> with_leader(conn, &handle_get(&1, key))
        "DELETE" -> with_leader(conn, &handle_delete(&1, key))
        _ -> send_resp(conn, 405, "method not allowed")
      end
    end
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

  # ── Leader forwarding ─────────────────────────────────────────────────────

  defp with_leader(conn, fun) do
    state = Server.Raft.get_state()

    cond do
      state.role == :leader ->
        fun.(conn)

      Server.Raft.leader_known?(state) ->
        qs = if conn.query_string != "", do: "?#{conn.query_string}", else: ""
        location = "http://#{state.leader_id}#{conn.request_path}#{qs}"

        conn
        |> put_resp_header("location", location)
        |> send_resp(307, "")

      true ->
        send_resp(conn, 503, "service unavailable")
    end
  end

  # ── KV handlers ───────────────────────────────────────────────────────────

  defp handle_put(conn, key) do
    {:ok, body, conn} = Plug.Conn.read_body(conn)

    if body == "" do
      send_resp(conn, 400, "value cannot be empty")
    else
      Server.KVStore.put(key, body)
      send_resp(conn, 200, "")
    end
  end

  defp handle_get(conn, key) do
    case Server.KVStore.get(key) do
      {:ok, value} -> send_resp(conn, 200, value)
      :error -> send_resp(conn, 404, "key not found")
    end
  end

  defp handle_delete(conn, key) do
    Server.KVStore.delete(key)
    send_resp(conn, 200, "")
  end
end
