defmodule Cassandra do
  import Cassandra.Utils
  
  def generate_data_seconds() do
    sensor_id = :uuid.string_to_uuid("e3e3b1b8-02de-4986-9900-5f8f21eff3e0")
    date = 1446336000000
    metadata = "AAAF"

    {:ok, client} = :cqerl.new_client()
    query = cql_query(statement: "INSERT INTO magpie.measurements (sensor_id, date, timestamp, metadata, value) VALUES (?, ?, ?, ?, ?);")  

    Enum.each(0..30, fn(day) -> 
      today = date + (day * 86400000)
      queries = for x <- 0..86399 do
        timestamp = today + x
        cql_query(query, values: [sensor_id: sensor_id, date: today, timestamp: timestamp, metadata: metadata, value: x])  
      end
      batch_query_1 = cql_query_batch(mode: 1, consistency: 1, queries: Enum.take(queries, 40000))
      {:ok, result} = :cqerl.run_query(client, batch_query_1)
      batch_query_2 = cql_query_batch(mode: 1, consistency: 1, queries: Enum.slice(queries, 40000, 1000000))
      IO.puts("Done seconds #{day}")
      {:ok, result} = :cqerl.run_query(client, batch_query_2)
      :timer.sleep(1000)
    end)
  end

  def generate_data_minutes() do
    sensor_id = :uuid.string_to_uuid("e3e3b1b8-02de-4986-9900-5f8f21eff3e0")
    month = 1446336000000
    metadata = "AAAF"

    {:ok, client} = :cqerl.new_client()
    query = cql_query(statement: "INSERT INTO magpie.measurements_by_minute (sensor_id, month, timestamp, avg, min, max, count) VALUES (?, ?, ?, ?, ?, ?, ?);")  
   
    queries = for x <- (0..43199) do
      timestamp = month + (x * 60000)
      cql_query(query, values: [sensor_id: sensor_id, month: month, timestamp: timestamp, avg: x, min: x, max: x, count: 60])  
    end
    batch_query = cql_query_batch(mode: 1, consistency: 1, queries: queries)
    {:ok, result} = :cqerl.run_query(client, batch_query)
    IO.puts("Done minutes")
  end

  def generate_data_hours() do
    sensor_id = :uuid.string_to_uuid("e3e3b1b8-02de-4986-9900-5f8f21eff3e0")
    timestamp = 1446336000000
    metadata = "AAAF"

    {:ok, client} = :cqerl.new_client()
    query = cql_query(statement: "INSERT INTO magpie.measurements_by_hour (sensor_id, timestamp, avg, min, max, count) VALUES (?, ?, ?, ?, ?, ?);")

    queries = for x <- (0..720) do
      timestamp = timestamp + (x * 3600000)
      cql_query(query, values: [sensor_id: sensor_id, timestamp: timestamp, avg: x, min: x, max: x, count: 60])  
    end
    batch_query = cql_query_batch(mode: 1, consistency: 1, queries: queries)
    {:ok, result} = :cqerl.run_query(client, batch_query)
    IO.puts("Done hours")
  end

  def erlcass_get_one() do
    sensor_id = "e3e3b1b8-02de-4986-9900-5f8f21eff3e0"
    date = 1448496000000

    :erlcass.execute(:get_measurements, [sensor_id, date])

    # time = Time.measure(fn -> Cassandra.erlcass_get_one() end)
    # IO.inspect(time)
  end


  def erlcass_start() do
    :ok = Application.start(:erlcass)
    :ok = :erlcass.set_cluster_options(contact_points: <<"192.168.1.104">>)
    :ok = :erlcass.create_session([])
    :ok = :erlcass.add_prepare_statement(:get_measurements, <<"SELECT sensor_id, timestamp, value, metadata FROM magpie.measurements WHERE sensor_id = ? AND date = ?">>)
    :ok = :erlcass.add_prepare_statement(:get_measurement, <<"SELECT * FROM magpie.measurements WHERE sensor_id = ? AND date = ? LIMIT 1;">>)
  end

  def erlcass_get(sensor_id, date) do
    # sensor_id must be string, date must be timestamp (int, milliseconds since epoch)
    
    :erlcass.execute(:get_measurements, [sensor_id, date])
  end

  def get(sensor_id, date) do
    Stream.resource(
      fn() -> 
        {:ok, client} = :cqerl.new_client()
        query = cql_query(
          statement: "SELECT * FROM magpie.measurements WHERE sensor_id = ? AND date= ? ;",
          values: [sensor_id: :uuid.string_to_uuid(sensor_id), date: date],
          page_size: 60000)
        {:ok, result} = :cqerl.run_query(client, query)
        result
      end,
      fn(result) ->
        case :cqerl.next(result) do
          {row, next_result} ->
            {[row], next_result}
          :empty_dataset -> 
            case :cqerl.fetch_more(result) do
              {:ok, next_result} ->
                case :cqerl.next(next_result) do
                  {row, next_result} -> {[row], next_result} 
                  :empty_dataset -> 
                    {:halt, result}
                end
              :no_more_result -> 
                {:halt, result}
            end
        end
      end,
      fn(result) ->
        client = elem(result, 4)
        :ok = :cqerl.close_client(client)
      end
    )
  end

  def to_measurement(row) do
    [timestamp: row[:timestamp], value: row[:value]]
  end
end
