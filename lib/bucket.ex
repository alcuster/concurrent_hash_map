defmodule Bucket do
  @moduledoc """
  An extension of GitHub user pnpranavrao's implementation of a bucket
  for a concurrent hash map in Elixir. The underlying data structure for each
  bucket is a List, and each List stores integer keys associated with values of
  any type.

  Author: Alec Custer

  Original implementation: https://github.com/pnpranavrao/concurrent_hash_map
  """
  use GenServer
  @max_limit 4 #Max length of List in a bucket

  #Client Functions

  @doc """
  Initiate the GenServer that represents the bucket.
  """
  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  #Callbacks / Server-side Functions

  @doc """
  Initializes the GenServer representing the bucket and sets its initial
  parameters.

  Parameters:
  list: The contents of the bucket, represented as a List of key, value pairs

  parent_pid: The process that created the bucket; i.e. the HashMap that a
              bucket belongs to.

  element_count: The number of elements currently in the bucket

  generation: The number of times that the HashMap that a bucket belongs to has
              been resized.

  counter: A Counter used for benchmarking. The original author used this
           module to test his/her own resizing implementation. It is not fully
           compatibile with my own implementation.
  """
  def init([{:parent_pid, pid}, {:generation, generation}, {:counter, counter}]) do
    state = %{
      list: [],
      parent_pid: pid,
      element_count: 0,
      generation: generation,
      counter: counter
   }
   {:ok, state}
  end

  #Not used in my implementation
  def terminate(_reason, _state) do
    :ok
  end

  #Not used in my implementation
  def handle_call({:dump_all}, _from, state) do
    {:reply, dump_all(state), state}
  end

  #Not used in my implementation
  def handle_cast({:stop}, state) do
    {:stop, :normal, state}
  end

  @doc """
  Callback function for a synchronous get operation sent by the parent HashMap.
  Calls "get" on the supplied key and returns a response.
  """
  def handle_call({:get, key}, _from, state) do
    GenServer.cast(state[:counter], :get)
    {:reply, get(key, state), state}
  end

  @doc """
  Callback function for a set operation sent by the parent HashMap.
  Calls "set" on the supplied key and value and updates the state to reflect
  the result.
  """
  def handle_cast({:set, key, val}, state) do
    new_state = set({key, val}, state)
    GenServer.cast(state[:counter], :set)
    {:noreply, new_state}
  end

  #Belongs to original author's implementation of re-hashing elements.
  def handle_cast({:re_set, key, val}, state) do
    new_state = set({key, val}, state)
    {:noreply, new_state}
  end

  @doc """
  Callback for handling a delete message sent by the parent HashMap. Calls
  delete function and returns the new state.
  """
  def handle_cast({:delete, key, resizing}, state) do
    new_state = delete(key, resizing, state)
    GenServer.cast(state[:counter], :delete)
    {:noreply, new_state}
  end

  @doc """
  Callback for handling a move message sent by the parent HashMap. Calls
  move function and returns the new state.

  Note that this function also receives the tuple of buckets in table 2, so
  that it can forward the "send" calls to table 2's buckets on its own without
  relaying back to the parent HashMap.
  """
  def handle_cast({:move, move_count, bucket_id, table2_buckets}, state) do
    new_state = move(move_count, bucket_id, table2_buckets, state)
    {:noreply, new_state}
  end

  #Optional specification for sending messages between processes. Not used.
  # def handle_info(_msg, state) do
  #   {:noreply, state}
  # end

  @doc """
  Given a key and a value, prepend it to the bucket's list of contents if it
  is not already present in the list.

  If after inserting an element and incrementing element_counter the new size
  of the bucket has reached @max_limit, send a message to the parent HashMap
  alerting it that this bucket is overflowing.
  """
  def set(key_value, %{list: list, element_count: count} = state) do
    %{new_list: new_list, exists: exists} =
    list
    |> Enum.reduce(%{new_list: [], exists: false},
      fn({key, val}, acc) ->
        if key == elem(key_value, 0) do
          changes = %{new_list: [key_value | acc[:new_list]], exists: true}
          Map.merge(acc, changes)
        else
          changes = %{new_list: [{key, val} | acc[:new_list]]}
          Map.merge(acc, changes)
        end
      end)
    if exists do
      %{state | list: new_list}
    else
      # If count has reached @max_element, send a message to the parent HashMap.
      if count == @max_limit do
        GenServer.cast(state[:parent_pid], {:bucket_overflow, state[:generation]})
      end
      # Prepend the element to the list.
      %{state | list: [key_value | list], element_count: count+1}
    end
  end

  @doc """
  Searches the buckets list of elements for an element with the given key.

  Returns: The value of the element if it is found; nil otherwise.
  """
  def get(req_key, %{list: list}) do
    result =
    list |> Enum.find(nil, fn({key, val}) -> req_key == key end)
    case result do
      nil -> nil
      {key, val} -> val
    end
  end

  @doc """
  Removes an element with the supplied key from the bucket, if that element
  exists. Otherwise, do nothing. If we are in a resize phase and do not find
  the element, it may exist in the second hash map, so check that table, too.
  """
  def delete(req_key, resizing, %{list: list, element_count: count} = state) do
    IO.inspect(list)
    #Go through the list looking for the given key.
    %{new_list: new_list, flag: flag} =
    list |> Enum.reduce(%{new_list: [], flag: false},
      fn({key, val}, acc) -> if (key == req_key) do
                               %{acc | flag: true} #Set flag if key is found
                             else
                               %{acc | new_list: [{key, val} | acc[:new_list]]}
                             end
      end)
    if flag do
      IO.puts("Deleting #{req_key}")
      IO.inspect(new_list)

      #Create map reflecting changed state, then merge it with the
      #old state and return the result.
      changes = %{list: new_list, element_count: count-1}
      Map.merge(state, changes)
    else
      #If in resize phase and didn't find element, should check table 2.
      if resizing do
        GenServer.cast(state[:parent_pid], {:table2_delete, req_key})
      end
      #If not resizing, then it isn't in the bucket. Return unchanged state.
      state
    end
  end

  @doc """
  Copies move_count items in a bucket to their correct locations in the second
  HashMap. The bucket is responsible for calculating the location using
  hash_function and for sending the GenServer casts to the corresponding
  buckets.

  After sending the elements to be inserted in the new HashMap,
  discards those elements. If doing so brings the size of the bucket to zero,
  then this bucket is done with its portion of the resizing phase and should
  notify the parent HashMap.

  Returns: Nothing to the client, but updates state.
  """
  def move(move_count, bucket_id, table2_buckets, %{list: list, element_count: count} = state) do
    IO.inspect(table2_buckets)
    IO.puts("Moving. My id is #{bucket_id}")

    bucket_count = tuple_size(table2_buckets)
    split_list = Enum.split(list, move_count)

    elem(split_list, 0)|> Enum.each(fn({key, val}) ->
                            new_bucket_id = hash_function(key, bucket_count)
                            new_bucket = elem(table2_buckets, new_bucket_id)
                            IO.puts("Moving {#{key}, #{val}}")
                            GenServer.cast(new_bucket, {:set, key, val})
                            IO.puts("Rehashing key #{key} from #{bucket_id} to #{new_bucket_id}")
                          end)
    changes = %{list: elem(split_list, 1), element_count: max(count - move_count, 0) }
    if (changes[:element_count] == 0) do
      IO.puts("Done moving my elements to new table")
      GenServer.cast(state[:parent_pid], {:done_moving, bucket_id})
    end
    Map.merge(state, changes)
  end

  #Not used in my implementation
  def dump_all(state) do
    state[:list]
  end

  @doc """
  The hash function used to find the correct bucket for
  getting/setting/deleting elements.

  Returns: the key value modulo the number of buckets.
  """
  def hash_function(key, bucket_count) do
    rem(key, bucket_count)
  end

end
