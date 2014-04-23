require 'connection_pool'

class ConnectionPool::Multi
  VERSION = '1.0'

  def initialize options = {}, &block
    raise ArgumentError, 'Connection pool requires a block' unless
      block_given?

    @size    = options.fetch :size,    ConnectionPool::DEFAULTS[:size]
    @timeout = options.fetch :timeout, ConnectionPool::DEFAULTS[:timeout]

    @available = ConnectionPool::Multi::TimedStack.new @size, &block
    @key = :"current-#{@available.object_id}"
  end

  def with options = {}
    conn = checkout options
    begin
      yield conn
    ensure
      checkin
    end
  end

  def checkout options = {}
    stack = Thread.current[@key] ||= []

    if stack.empty? then
      timeout = options[:timeout] || @timeout
      conn = @available.pop timeout: timeout
    else
      conn = stack.last
    end

    stack.push conn

    conn
  end

  def checkin
    stack = Thread.current[@key]

    raise ConnectionPool::Error, 'no connections are checked out' if
      !stack || stack.empty?

    conn = stack.pop

    @available << conn if stack.empty?

    nil
  end

  def shutdown &block
    @available.shutdown(&block)
  end

end

require 'connection_pool/multi/timed_stack'
