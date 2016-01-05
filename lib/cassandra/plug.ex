defmodule Cassandra.Plug do
  import Plug.Conn
  use Timex

  def start() do
    {:ok, _} = Plug.Adapters.Cowboy.http(Cassandra.Plug, [])
  end

  def init(opts) do
    opts
  end

  def call(conn, _opts) do
    

    do_lazy(conn)
    # do_eager(conn)
    # do_erlcass(conn)

    
    # time = Time.measure(fn -> Cassandra.get |> Enum.map(fn(m) -> "#{m[:timestamp]},#{m[:value]}\n" end) |> Enum.into(conn) end)
    # IO.inspect(time)
  end

  # def do_erlcass(conn) do
  #   {:ok, measurements} = Cassandra.erlcass_get("e3e3b1b8-02de-4986-9900-5f8f21eff3e0", 1448496000000)
    
  #   data = Enum.reduce(measurements, "", fn(m, acc) ->
  #     timestamp = elem(m, 0)
  #     value = elem(m, 1) 
  #     acc <> "#{timestamp},#{value}\n" 
  #   end)

  #   conn
  #   |> put_resp_content_type("text/plain")
  #   |> send_resp(200, data)
  # end

  # get(query) <- DA core
  # |> Stream.map(to_model) <- DA wrapper
  # |> Stream.take(1) <- controller

  def do_lazy(conn) do
    conn = send_chunked(conn, 200)
    Cassandra.get("e3e3b1b8-02de-4986-9900-5f8f21eff3e0", 1448496000000)
    |> Stream.map(fn(m) -> "#{m[:timestamp]},#{m[:value]}\n" end) 
    |> Stream.chunk(100, 100, []) 
    |> Enum.into(conn)
  end

  def do_eager(conn) do
    measurements = Cassandra.get("e3e3b1b8-02de-4986-9900-5f8f21eff3e0", 1448496000000)
    data = Enum.reduce(measurements,"", fn(m, acc) ->
      acc <> "#{m[:timestamp]},#{m[:value]}\n"
    end)
    
    conn
    |> put_resp_content_type("text/plain")
    |> send_resp(200, data)
  end
end