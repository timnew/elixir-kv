require IEx

defmodule KV.Bucket.Supervisor do
  use Supervisor

  @name __MODULE__

  def start_link do
    Supervisor.start_link(@name, :ok, name: @name)
  end

  def start_bucket do
    Supervisor.start_child(@name, [])
  end

  def init(:ok) do
    children = [
      worker(KV.Bucket, [], restart: :temporary)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end
end
