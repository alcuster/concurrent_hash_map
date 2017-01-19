defmodule HashMapTest do
  use ExUnit.Case
  doctest HashMap

  setup do
    {:ok, map} = HashMap.start_link
    {:ok, %{map: map}}
  end

  test "basic set,get,delete works", context do
    map = context[:map]

    #Returns nil because we haven't inserted anything yet
    assert HashMap.get(map, 3) == nil

    #Insert some elements
    HashMap.set(map, 3, "Three")
    HashMap.set(map, 78, "Seventy-two")
    HashMap.set(map, 1, "One")

    #Get an element
    assert HashMap.get(map, 3) == "Three"

    #Delete an element and check that it has been removed
    HashMap.delete(map, 3)
    assert HashMap.get(map, 3) == nil

    #Test doubling the table
    assert HashMap.get_bucket_count(map) == 4
    HashMap.new_table(map)
    assert HashMap.get_flags(map) == [false, false, false, false]
    HashMap.set(map, 70, "Seventy")


    #Check that the second table has been created with twice as many buckets
    assert HashMap.get_bucket2_count(map) == 8

    assert HashMap.resizing?(map) == true
    assert HashMap.get(map, 70) == "Seventy"

    #Check that inserting 70 triggered a "move" operation
    assert HashMap.get_flags(map) == [false, false, true, false]

    HashMap.delete(map, 70)
    assert HashMap.get(map, 3) == nil

    assert HashMap.get(map, 70) == nil #May fail if the deletion hasn't finished
  end
end
