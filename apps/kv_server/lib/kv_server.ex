defmodule KVServer do
  require Logger

  def accept(port) do
    # The options below mean:
    #
    # 1. `:binary` - receives data as binaries (instead of lists)
    # 2. `packet: :line` - receives data line by line
    # 3. `active: false` - blocks on `:gen_tcp.recv/2` until data is available
    # 4. `reuseaddr: true` - allows us to reuse the address if the listener crashes
    #
    {:ok, socket} = :gen_tcp.listen(port,
                      [:binary, packet: :line, active: false, reuseaddr: true])
    Logger.info "Accepting connections on port #{port}"
    loop_acceptor(socket)
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    IO.puts "Incoming #{inspect client}"
    {:ok, pid} = Task.Supervisor.start_child(KVServer.TaskSupervisor, fn ->
      :gen_tcp.send(client, "READY\r\n")
      serve(client)
    end)
    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end

  defp serve(socket) do
    with({:ok, line} <- request(socket),
         {:ok, command} <- KVServer.Command.parse(line),
         do: KVServer.Command.run(command))
    |> respond(socket)

    serve(socket)
  end

  defp request(socket) do
    :gen_tcp.recv(socket, 0)
  end

  defp respond({:ok, line}, socket) do
    :gen_tcp.send(socket, line)
  end

  defp respond({:action, :close}, socket) do
    :gen_tcp.send(socket, "BYE\r\n")
    :gen_tcp.close(socket)

    shutdown(socket)
  end

  defp respond({:error, :unknown_command}, socket) do
    # Known error. Write to the client.
    :gen_tcp.send(socket, "UNKNOWN COMMAND\r\n")
  end

  defp respond({:error, :not_found}, socket) do
    :gen_tcp.send(socket, "NOT FOUND\r\n")
  end

  defp respond({:error, :closed}, socket) do
    shutdown(socket)
  end

  defp respond({:error, error}, socket) do
    # Unknown error. Write to the client and exit.
    IO.puts("Error: #{inspect error}")
    :gen_tcp.send(socket, "ERROR\r\n")
  end

  defp shutdown(socket) do
    IO.puts "Terminated #{inspect socket}"
    exit(:shutdown)
  end
end
