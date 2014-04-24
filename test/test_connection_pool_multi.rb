require 'minitest/autorun'

require 'connection_pool/multi'

class TestConnectionPoolMulti < Minitest::Test

  class NetworkConnection
    attr_reader :host

    def initialize host = nil
      @host = host
      @x = 0
    end

    def do_something
      @x += 1
      sleep 0.05
      @x
    end

    def fast
      @x += 1
    end

    def do_something_with_block
      @x += yield
      sleep 0.05
      @x
    end

    def respond_to?(method_id, *args)
      method_id == :do_magic || super(method_id, *args)
    end
  end

  class Recorder
    def initialize
      @calls = []
    end

    attr_reader :calls

    def do_work(label)
      @calls << label
    end
  end

  def use_pool(pool, size)
    size.times.map do
      Thread.new do
        pool.with 'a.example' do sleep end
      end
    end.each do |thread|
      Thread.pass until thread.status == 'sleep'
    end.map do |thread|
      thread.kill
      Thread.pass while thread.alive?
    end
  end

  def test_checkin
    pool = ConnectionPool::Multi.new(timeout: 0, size: 1) { NetworkConnection.new }
    conn = pool.checkout

    t1 = Thread.new do
      pool.checkout
    end

    assert_raises Timeout::Error do
      t1.join
    end

    pool.checkin

    t2 = Thread.new do
      pool.checkout
    end

    assert_same conn, t2.value
  end

  def test_checkin_never_checkout
    pool = ConnectionPool::Multi.new(timeout: 0, size: 1) { Object.new }

    e = assert_raises ConnectionPool::Error do
      pool.checkin
    end

    assert_equal 'no connections are checked out', e.message
  end

  def test_checkin_no_current_checkout
    pool = ConnectionPool::Multi.new(timeout: 0, size: 1) { Object.new }

    pool.checkout
    pool.checkin

    assert_raises ConnectionPool::Error do
      pool.checkin
    end
  end

  def test_checkin_twice
    pool = ConnectionPool::Multi.new(timeout: 0, size: 1) { Object.new }

    pool.checkout
    pool.checkout

    pool.checkin

    assert_raises Timeout::Error do
      Thread.new do
        pool.checkout
      end.join
    end

    pool.checkin

    assert Thread.new { pool.checkout }.join
  end

  def test_checkout
    pool = ConnectionPool::Multi.new(size: 1) { NetworkConnection.new }

    conn = pool.checkout

    assert_kind_of NetworkConnection, conn

    assert_same conn, pool.checkout
  end

  def test_checkout_multithread
    pool = ConnectionPool::Multi.new(size: 2) { NetworkConnection.new }
    conn = pool.checkout

    t = Thread.new do
      pool.checkout
    end

    refute_same conn, t.value
  end

  def test_checkout_nested
    recorder = Recorder.new
    pool = ConnectionPool::Multi.new(size: 1) { recorder }
    pool.with 'a.example' do |r_outer|
      @other = Thread.new do |t|
        pool.with 'a.example' do |r_other|
          r_other.do_work('other')
        end
      end

      pool.with 'a.example' do |r_inner|
        r_inner.do_work('inner')
      end

      Thread.pass

      r_outer.do_work('outer')
    end

    @other.join

    assert_equal ['inner', 'outer', 'other'], recorder.calls
  end

  def test_checkout_shutdown
    pool = ConnectionPool::Multi.new(size: 1) { true }

    pool.shutdown { }

    assert_raises ConnectionPool::PoolShuttingDownError do
      pool.checkout
    end
  end

  def test_checkout_timeout
    pool = ConnectionPool::Multi.new(timeout: 0, size: 0) { Object.new }

    assert_raises Timeout::Error do
      pool.checkout
    end
  end

  def test_checkout_timeout_override
    pool = ConnectionPool::Multi.new(timeout: 0, size: 1) { NetworkConnection.new }

    thread = Thread.new do
      pool.with 'a.example' do |net|
        net.do_something
        sleep 0.01
      end
    end

    Thread.pass while thread.status == 'run'

    assert_raises Timeout::Error do
      pool.checkout
    end

    assert pool.checkout timeout: 0.1
  end

  def test_shutdown
    recorders = []

    pool = ConnectionPool::Multi.new(size: 3) do
      Recorder.new.tap { |r| recorders << r }
    end

    use_pool pool, 3

    pool.shutdown do |recorder|
      recorder.do_work("shutdown")
    end

    assert_equal [["shutdown"]] * 3, recorders.map { |r| r.calls }
  end

  def test_shutdown_blocks_on_in_use
    recorders = []

    pool = ConnectionPool::Multi.new(size: 3) do
      Recorder.new.tap { |r| recorders << r }
    end

    use_pool pool, 3

    pool.checkout connection_args: 'a.example'

    pool.shutdown do |recorder|
      recorder.do_work("shutdown")
    end

    assert_equal [[], ["shutdown"], ["shutdown"]], recorders.map { |r| r.calls }.sort

    pool.checkin

    assert_equal [["shutdown"], ["shutdown"], ["shutdown"]], recorders.map { |r| r.calls }
  end

  def test_threading_basic
    pool = ConnectionPool::Multi.new(size: 5) { NetworkConnection.new }

    threads = 15.times.map do
      Thread.new do
        pool.with 'a.example' do |net|
          net.do_something
        end
      end
    end

    a = Time.now
    result = threads.map(&:value)
    b = Time.now
    assert_operator((b - a), :>, 0.125)
    assert_equal([1,2,3].cycle(5).sort, result.sort)
  end

  def test_threading_heavy
    pool = ConnectionPool::Multi.new(timeout: 0.5, size: 3) { NetworkConnection.new }

    threads = 15.times.map do
      Thread.new do
        pool.with 'a.example' do |net|
          sleep 0.01
        end
      end
    end

    threads.map { |thread| thread.join }
  end

  def test_timeout
    pool = ConnectionPool::Multi.new(timeout: 0, size: 1) { NetworkConnection.new }
    thread = Thread.new do
      pool.with 'a.example' do |net|
        net.do_something
        sleep 0.01
      end
    end

    Thread.pass while thread.status == 'run'

    assert_raises Timeout::Error do
      pool.with 'a.example' do |net| net.do_something end
    end

    thread.join

    pool.with 'a.example' do |conn|
      refute_nil conn
    end
  end

  def test_with
    pool = ConnectionPool::Multi.new(timeout: 0.1, size: 1) { NetworkConnection.new }
    result = pool.with 'a.example' do |net|
      net.fast
    end
    assert_equal 1, result
  end

  def test_with_reuse
    pool = ConnectionPool::Multi.new(size: 5) { NetworkConnection.new }

    ids = 10.times.map do
      pool.with 'a.example' do |c| c.object_id end
    end

    assert_equal 1, ids.uniq.size
  end

  def test_with_timeout
    pool = ConnectionPool::Multi.new(timeout: 0, size: 1) { Object.new }

    pool.with 'a.example' do
      assert_raises Timeout::Error do
        Thread.new { pool.checkout }.join
      end
    end

    assert Thread.new { pool.checkout }.join
  end

  def test_with_timeout_override
    pool = ConnectionPool::Multi.new(timeout: 0, size: 1) { NetworkConnection.new }

    t = Thread.new do
      pool.with 'a.example' do |net|
        net.do_something
        sleep 0.01
      end
    end

    Thread.pass while t.status == 'run'

    assert_raises Timeout::Error do
      pool.with 'a.example' do |net| net.do_something end
    end

    pool.with 'a.example', timeout: 0.1 do |conn|
      refute_nil conn
    end
  end

end
