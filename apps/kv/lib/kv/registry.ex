defmodule KV.Registry do
  use GenServer

  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: name)
  end

  @doc """
  Looks up the bucket pid for `name` stored in `server`.

  Returns `{:ok, pid}` if the bucket exists, `:error` otherwise.
  """
  def lookup(server, name) when is_atom(server) do
    case :ets.lookup(server, name) do
      [{^name, bucket}] -> {:ok, bucket}
      _ -> :error
    end
  end

  @doc """
  Ensures there is a bucket associated to the given `name` in `server`.
  """
  def create(server, name) do
    GenServer.call(server, {:create, name})
  end

  @doc """
  Stops the registry.
  """
  def stop(server) do
    GenServer.stop(server)
  end

  ## Server Callbacks
  def init(table) do
    names = :ets.new(table, [:named_table, :set, :protected, read_concurrency: true])
    refs  = %{}
    {:ok, {names, refs}}
  end

  def handle_call({:create, name}, _from,  {names, refs} = state) do
    case lookup(names, name) do
      {:ok, bucket} -> {:reply, bucket, state}
      :error ->
        {:ok, bucket} = KV.Bucket.Supervisor.start_bucket
        ref = Process.monitor(bucket)

        :ets.insert(names, {name, bucket})
        refs = Map.put(refs, ref, name)

        {:reply, bucket, {names, refs}}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {names, refs}) do
    {name, refs} = Map.pop(refs, ref)
    :ets.delete(names, name)
    {:noreply, {names, refs}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
