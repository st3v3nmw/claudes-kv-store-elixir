defmodule Server.Router do
  use Plug.Router

  plug :match
  plug :dispatch

  # Health check — polled by the test harness before tests run
  match "/health" do
    case conn.method do
      "GET" -> send_resp(conn, 200, "ok")
      _ -> send_resp(conn, 405, "method not allowed")
    end
  end

  # Clear all keys
  match "/clear" do
    case conn.method do
      "DELETE" ->
        Server.KVStore.clear()
        send_resp(conn, 200, "")

      _ ->
        send_resp(conn, 405, "method not allowed")
    end
  end

  # /kv with no key segment
  match "/kv" do
    case conn.method do
      m when m in ["GET", "PUT", "DELETE"] -> send_resp(conn, 400, "key cannot be empty")
      _ -> send_resp(conn, 405, "method not allowed")
    end
  end

  # /kv/ and /kv/:key (glob catches both)
  match "/kv/*glob" do
    key = Enum.join(conn.path_params["glob"], "/")

    if key == "" do
      case conn.method do
        m when m in ["GET", "PUT", "DELETE"] -> send_resp(conn, 400, "key cannot be empty")
        _ -> send_resp(conn, 405, "method not allowed")
      end
    else
      case conn.method do
        "PUT" -> handle_put(conn, key)
        "GET" -> handle_get(conn, key)
        "DELETE" -> handle_delete(conn, key)
        _ -> send_resp(conn, 405, "method not allowed")
      end
    end
  end

  match _ do
    send_resp(conn, 404, "not found")
  end

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
