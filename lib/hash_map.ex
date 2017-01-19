defmodule HashMap do
  @moduledoc """
  An extension of GitHub user pnpranavrao's implementation of a concurrent
  HashMap in Elixir. This extension modifies the table resizing algorithm
  from one which "stops the world" to allocate new buckets and rehash all
  elements, and replaces it with one that incrementally rehashes elements on
  each insertion.

  Author: Alec Custer

  Original implementation: https://github.com/pnpranavrao/concurrent_hash_map
  """
  use GenServer
  @bucket_count 4 #Initiate the bucket count to 4.

  #Client Functions

  @doc """
  Initiate the GenServer that represents the HashMap.
  """
  def start_link do
    GenServer.start_link(__MODULE__, [])
  end

  @doc """
  Given the HashMap's process id and a key, finds and returns
  the element in the map associated with that key. If the key does not exist,
  then returns nil.
  """
  def get(pid, key) do
    GenServer.call(pid, {:get, key})
  end

  @doc """
  Given the HashMap's process id, a key, and a value, inserts that key, value
  pair into the HashMap.
  """
  def set(pid, key, value) do
    GenServer.cast(pid, {:set, key, value})
  end

  @doc """
  Given the HashMap's process id and a key, delete that key from the HashMap.
  """
  def delete(pid, key) do
    GenServer.cast(pid, {:delete, key})
  end


  # Testing functions

  @doc """
  For testing purposes. Returns: the number of buckets in the HashMap.
  """
  def get_bucket_count(pid) do
    GenServer.call(pid, {:get_bucket_count})
  end

  @doc """
  For testing purposes.

  Returns:
  | nil (If the HashMap is not in the process of resizing.)
  | The number of buckets in the second, "new" table. (Otherwise)
  """
  def get_bucket2_count(pid) do
    GenServer.call(pid, {:get_bucket2_count})
  end

  @doc """
  For testing purposes.

  Returns:
  | nil (If the HashMap is not in the process of resizing.)
  | The list of flags used to keep track of which buckets have finished
    moving their contents to the new HashMap. (Otherwise)
  """
  def get_flags(pid) do
    GenServer.call(pid, {:get_flags})
  end

  @doc """
  For testing purposes.

  Returns
  """
  def resizing?(pid) do
    GenServer.call(pid, {:resizing})
  end

  @doc """
  For testing purposes.

  Manually initiates the resize phase (detailed description in callback
  function documentation).
  """
  def new_table(pid) do
    GenServer.call(pid, {:new_table})
  end


  #Callbacks / Server-side Functions

  @doc """
  Initializes the GenServer representing the HashMap and
  sets its initial parameters.

  Paremeters:
  bucket_count: The number of buckets in the HashMap. Initialized to the
                a previously set parameter, @bucket_count

  table2_bucket_count: Initialized to nil. During a resize phase, this is set
                       to the number of buckets in the new, larger table.

  buckets: A tuple of child GenServers, each representing a bucket in the hash
           map. Initialized using a function init_buckets that creates
           @bucket_count new buckets.

  table2_buckets: Initialized to nil. During a resize phase, this is set using
                  init_buckets to a tuple of buckets twice the size of the
                  original table.

  overfill_count: The number of elements that are overfilling our table.
                  This number is incremented every time a bucket inserts an
                  element that causes its length to be great than the specified
                  max bucket length. If this exceeds bucket_count/2 we trigger
                  a resize phase.

  generation: The number of times the table has been resized. Used to ensure
              we don't initiate a resize while already in the process of one.

  counter: A Counter used for benchmarking. The original author used this
           module to test his/her own resizing implementation. It is not fully
           compatibile with my own implemetation.

  resizing: A boolean flag indicating whether we are currently in the resize
            phase. Set to true at the start of a resize, and reset to false
            after all buckets have moved their contents into the new table.

  moving_flags: A list of boolean flags indicating which buckets have finished
                moving their contents to the new HashMap. Initialized to nil;
                at the start of a resize phase is set to a list of
                table2_bucket_count false falgs.
  """
  def init([]) do
    {:ok, counter} = Counter.start_link
    state = %{
      bucket_count: @bucket_count,
      table2_bucket_count: nil,
      buckets: init_buckets(@bucket_count, 0, counter),
      table2_buckets: nil,
      overfill_count: 0,
      generation: 0,
      counter: counter,
      resizing: false,
      moving_flags: nil
    }
    {:ok, state}
  end

  @doc """
  Handle a synchronous client "get" call. If we are resizing and we don't
  find the desired value in the first map, then we should check the new/second
  map as well.

  Returns a tuple of the specified format for Genserver.call requests,
  containing a :reply atom, a response (in this case the value for the key
  we're getting, or nil if it doesn't exist), and the new state (in this case
  unchanged).
  """
  def handle_call({:get, key}, _from, state) do
    #Find the correct bucket using the hash function.
    bucket_id = hash_function(key, state[:bucket_count])

    #Get that bucket's process id from the list of buckets.
    bucket = elem(state[:buckets], bucket_id)

    #Send a "get" call to the bucket and store the response.
    response = GenServer.call(bucket, {:get, key})

    #If we're resizing and we didn't find the key, it may have been inserted
    #since the start of the resize phase and would thus reside in the second
    #table.
    if (state[:resizing] and response == nil) do
      IO.puts("Did not find #{key} in first table. Checking second table.")
      #Find the correct bucket in the new HashMap.
      new_bucket_id = hash_function(key, state[:table2_bucket_count])

      #Send a "get" call to the correct bucket and store the response.
      new_bucket = elem(state[:table2_buckets], new_bucket_id)
      new_response = GenServer.call(new_bucket, {:get, key})

      #Return the response to the client.
      {:reply, new_response, state}
    else
      #If we're not resizing or we found the element in the first map, just
      #return the original response.
      {:reply, response, state}
    end
  end

  @doc """
  Handles an asynchronous client "set" call. Given a key and value, inserts
  that key and value into the correct bucket in the map. If we are in a
  resizing phase, all new insertions are forwarded to the new map.

  Returns: Nothing to the client, but updates the GenServer's state.
  """
  def handle_cast({:set, key, value}, state) do
    #Find the given key's bucket in the first/original HashMap.
    bucket_id = hash_function(key, state[:bucket_count])
    bucket = elem(state[:buckets], bucket_id)

    #If we're in a resize phase, only add elements to the new table.
    if (state[:resizing]) do
      #Find and get the bucket in the second table
      new_bucket_id = hash_function(key, state[:table2_bucket_count]) #hash into the new table
      new_bucket = elem(state[:table2_buckets], new_bucket_id)

      IO.puts("Inserting #{key} in bucket # #{new_bucket_id} in second table")

      #Forward the "set" cast to new_bucket
      GenServer.cast(new_bucket, {:set, key, value})

      #Copy r = bucket_count/2 elements into the new table.
      move_count = div(state[:bucket_count], 2) # Calculate r
      IO.puts("Moving #{move_count} elements to new table.")

      #Forward a "move" cast to the key's corresponding bucket in the first
      #table. Note that this assumes evenly dispersed insertions/deletions
      #among the buckets.
      GenServer.cast(bucket, {:move, move_count, bucket_id, state[:table2_buckets]})
    else
      #If not resizing, just insert into the first/only HashMap.
      IO.puts("Inserting in bucket # #{bucket_id}")
      GenServer.cast(bucket, {:set, key, value})
    end
    {:noreply, state}
  end


  @doc """
  Handle a client request to delete a given key (and its value)
  from the HashMap.
  """
  def handle_cast({:delete, key}, state) do
    bucket_id = hash_function(key, state[:bucket_count])
    bucket = elem(state[:buckets], bucket_id)
    GenServer.cast(bucket, {:delete, key, state[:resizing]})
    {:noreply, state}
  end

  @doc """
  Handle a message from a bucket indicating that we are in a resize phase and
  did not find the element in table 1, so we should now check table 2.
  """
  def handle_cast({:table2_delete, key}, state) do
    IO.puts("Looking in second table")
    new_bucket_id = hash_function(key, state[:table2_bucket_count])
    new_bucket = elem(state[:table2_buckets], new_bucket_id)
    #Flip the resizing key to false to avoid repeatedly calling table2_delete
    GenServer.cast(new_bucket, {:delete, key, false})
    {:noreply, state}
  end

  @doc """
  Testing/debugging function.

  Returns: bucket_count
  """
  def handle_call({:get_bucket_count}, _from, state) do
    {:reply, state[:bucket_count], state}
  end

  @doc """
  Testing/debugging function.

  Returns: table2_bucket_count
  """
  def handle_call({:get_bucket2_count}, _from, state) do
    {:reply, state[:table2_bucket_count], state}
  end

  @doc """
  Testing/debugging function.

  Returns: moving_flags
  """
  def handle_call({:get_flags}, _from, state) do
    {:reply, state[:moving_flags], state}
  end

  @doc """
  Testing/debugging function.

  Returns: resizing
  """
  def handle_call({:resizing}, _from, state) do
    {:reply, state[:resizing], state}
  end

  @doc """
  Callback for when a bucket declares that it has finished copying all of its
  elements to the second table. Switches that bucket's moving_flag to true and
  checks if all buckets are now done copying their contents (and are now empty).

  Returns: Nothing to the client, but updates the state.
  """
  def handle_cast({:done_moving, bucket_id}, state) do
    IO.puts("Bucket #{bucket_id} is done moving")
    new_flags = List.update_at(state[:moving_flags], bucket_id, fn(flag) -> true end)

    #Check if a false flag remains in the list of flags. If all flags are true..
    if (!Enum.member?(new_flags, false)) do
      #..resizing is done! Can now replace the old table with the new one.
      changes = %{
        #buckets is overwritten with table2_buckets
        buckets: state[:table2_buckets],
        table2_buckets: nil, # Reset table2_buckets to nil
        #bucket_count overwritten with table2_bucket_count
        bucket_count: state[:table2_bucket_count],
        table2_bucket_count: 0, # Reset table2_bucket_count to 0
        overfill_count: 0, #reset overfill_count
        counter: state[:counter],
        resizing: false, #Reset resizing flag
        moving_flags: nil #Reset moving_flags
      }
    else
      #If there's still a false flag, just add the updated list to the state
      changes = %{moving_flags: new_flags}
    end

    new_state = Map.merge(state, changes)
    {:noreply, new_state}
  end

  @doc """
  Callback for handling a bucket that declares its bucket is overflowing.
  Checks that the bucket that sent the message belongs to the parent's
  generation, and if so, increments overfill_count.

  Initiates the resize phase if the updated overfill_count is greater than
  our threshold, r = bucket_count/2.
  """
  def handle_cast({:bucket_overflow, generation}, state) do
    if generation == state[:generation] do
      state = Map.put(state, :overfill_count, state[:overfill_count]+1)
      if (state[:overfill_count] > state[:bucket_count]/2) do
        GenServer.call(self(), {:new_table})
      end
    end
    {:noreply, state}
  end

  @doc """
  Handles a synchronous call to create a new table of size bucket_count * 2.

  Returns: A tuple in the form specified for GenServer handle_call functions,
  containing a :reply atom, a response (in this case an :ok atom), and the
  updated state of the GenServer.
  """
  def handle_call({:new_table}, _from, state) do
    new_state = resize(state)

    {:reply, :ok, new_state}
  end

  #Original author's callback function for rehashing the entire HashMap
  def handle_cast({:rehash, generation}, state) do
    if generation == state[:generation] do
      new_state = rehash(state)
      {:noreply, new_state}
    else
      {:noreply, state}
    end
  end

  @doc """
  Given a HashMap state with bucket_count buckets, creates a new HashMap with
  bucket_count * 2 buckets, merges it with the original's state, and finally
  returns the result.
  """
  def resize(state) do
    #The number of buckets in the new table will be twice the old one.
    new_bucket_count = state[:bucket_count]*2
    #Initialize new_bucket_count buckets using init_buckets
    new_buckets = init_buckets(new_bucket_count, state[:generation]+1, state[:counter])

    #Create the list of new_bucket_count false moving flags
    flags = (1..state[:bucket_count]) |> Enum.map(fn _i -> false end)

    #Store the new values in a new state map, to be merged with the original.
    #Note that the buckets in the original table are left intact - this step
    #simply creates a new empty HashMap twice the size of the original.
    changes = %{
      table2_buckets: new_buckets,
      table2_bucket_count: new_bucket_count,
      #overfill_count: 0, #Reset overfill_count to zero. Maybe do this at done_moving?
      generation: state[:generation] + 1,
      counter: state[:counter],
      resizing: true, #Switch resizing flag to true
      moving_flags: flags #Set moving_flags to the list created above
    }

    #Update state by merging with changes, and return the result.
    Map.merge(state, changes)
  end

  #Optional specification for sending messages between processes. Not used.
  # def handle_info(_msg, state) do
  #   {:noreply, state}
  # end

  @doc """
  Initializes n buckets with a given generation and a counter.
  """
  def init_buckets(n, generation, counter) do
    (1..n)
    |> Enum.map(fn _i ->
      {:ok, bucket_pid} = Bucket.start_link([{:parent_pid, self()},
                                             {:generation, generation},
                                             {:counter, counter}])
      bucket_pid
    end)
    |> List.to_tuple
  end

  @doc """
  The hash function used to find the correct bucket for
  getting/setting/deleting elements.

  Returns: the key value modulo the number of buckets.
  """
  def hash_function(key, bucket_count) do
    rem(key, bucket_count)
  end

  @doc """
  This is the original author's solution for resizing the table. Once the
  threshold is surpassed, this implementation incurs a "stop the world"
  scenario where all buckets rehash their contents into bucket_count*2
  new buckets.

  My own implementation is an attempt to minimize the slowdown incurred when
  rehashing all buckets at once (as is done here) by instead dividing the work
  amongst multiple insertion calls.
  """
  def rehash(state) do
    IO.inspect "Rehashing to #{state[:bucket_count]*2}"
    new_bucket_count = state[:bucket_count]*2
    new_buckets = init_buckets(new_bucket_count, state[:generation]+1, state[:counter])
    Tuple.to_list(state[:buckets])
    |> Enum.each(fn(bucket) ->
      GenServer.call(bucket, {:dump_all}) #tell each bucket to dump their current contents (one by one)
      |> Enum.each(fn({key, val}) -> #pipe dumped contents here
        new_bucket_id = hash_function(key, new_bucket_count) #find the new bucket
        new_bucket = elem(new_buckets, new_bucket_id) #get the new bucket
        GenServer.cast(new_bucket, {:re_set, key, val}) #say GO to every bucket (insert all elements into new table now)
      end)
      GenServer.cast(bucket, {:stop})
    end)
    IO.inspect "Rehashing to #{state[:bucket_count]*2} done"
    #New State
    %{
      buckets: new_buckets,
      bucket_count: new_bucket_count,
      overfill_count: 0, #reset overfill_count
      generation: state[:generation] + 1,
      counter: state[:counter]
    }
  end
end

# Leftover test lines from original author's implementation.
# val = :value
# {:ok, map} = HashMap.start_link
# reader = fn(map) -> (1..500_000) |> Enum.each(&HashMap.get(map,&1)); IO.inspect "reading done" end
# writer = fn(map) -> (1..500_000) |> Enum.each(&HashMap.set(map,&1,val)); IO.inspect "writing done"end
