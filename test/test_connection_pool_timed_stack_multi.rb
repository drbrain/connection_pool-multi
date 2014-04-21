require 'minitest/autorun'

require 'connection_pool/multi'

class TestConnectionPoolMultiTimedStack < Minitest::Test

  class Connection
    attr_reader :host

    def initialize(host)
      @host = host
    end
  end

  def setup
    @stack = ConnectionPool::Multi::TimedStack.new do
      Object.new
    end
  end

  def test_empty_eh
    stack = ConnectionPool::Multi::TimedStack.new 1 do
      Object.new
    end

    refute_empty stack

    popped = stack.pop

    assert_empty stack

    stack.push connection_args: popped

    refute_empty stack
  end

  def test_length
    stack = ConnectionPool::Multi::TimedStack.new 1 do
      Object.new
    end

    assert_equal 1, stack.length

    popped = stack.pop

    assert_equal 0, stack.length

    stack.push connection_args: popped

    assert_equal 1, stack.length
  end

  def test_pop
    object = Object.new
    @stack.push object

    popped = @stack.pop

    assert_same object, popped
  end

  def test_pop_empty
    e = assert_raises Timeout::Error do
      @stack.pop timeout: 0
    end

    assert_equal 'Waited 0 sec', e.message
  end

  def test_pop_full
    stack = ConnectionPool::Multi::TimedStack.new 1 do
      Object.new
    end

    popped = stack.pop

    refute_nil popped
    assert_empty stack
  end

  def test_pop_wait
    thread = Thread.start do
      @stack.pop
    end

    Thread.pass while thread.status == 'run'

    object = Object.new

    @stack.push object

    assert_same object, thread.value
  end

  def test_pop_shutdown
    @stack.shutdown { }

    assert_raises ConnectionPool::PoolShuttingDownError do
      @stack.pop
    end
  end

  def test_push
    stack = ConnectionPool::Multi::TimedStack.new 1 do
      Object.new
    end

    conn = stack.pop

    stack.push connection_args: conn

    refute_empty stack
  end

  def test_push_shutdown
    called = []

    @stack.shutdown do |object|
      called << object
    end

    @stack.push connection_args: Object.new

    refute_empty called
    assert_empty @stack
  end

  def test_shutdown
    @stack.push connection_args: Object.new

    called = []

    @stack.shutdown do |object|
      called << object
    end

    refute_empty called
    assert_empty @stack
  end

  def test_pop_recycle
    stack = ConnectionPool::Multi::TimedStack.new 2 do |host|
      Connection.new(host)
    end

    a_conn = stack.pop connection_args: 'a.example'
    stack.push a_conn, connection_args: 'a.example'

    b_conn = stack.pop connection_args: 'b.example'
    stack.push b_conn, connection_args: 'b.example'

    c_conn = stack.pop connection_args: 'c.example'

    assert_equal 'c.example', c_conn.host

    stack.push c_conn, connection_args: 'c.example'

    recreated = stack.pop connection_args: 'a.example'

    refute_same a_conn, recreated
  end

end

