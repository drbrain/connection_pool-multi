require 'connection_pool'

##
# A multi-connection version of ConnectionPool.  This allows you to manage
# connections to different services while managing a more-abstract resource
# such as file descriptors.
#
# Reconnection is not handled by ConnectionPool::Multi.  If the remote end
# shuts down a connection while it was stored in the pool you must manually
# reconnect if the resource will not reconnect for you.
#
# Connections will be reused when the same connection arguments are given.
#
# Connections will be recycled by LRU if there are no matching connections in
# the pool and all connections are filled up.
#
# Example:
#
#   http_pool = ConnectionPool::Multi.new 50 do |hostname, port|
#     http = Net::HTTP.new hostname, port
#     http.start
#   end
#
#   http_pool.with ['www.example', 80] do |connection|
#     connection.get '/foo'
#   end
#
#   http_pool.with ['other.example', 80] do |connection|
#     connection.get '/bar'
#   end
#
#   http_pool.shutdown do |connection|
#     connection.finish
#   end
#
# Note that unlike ConnectionPool, ConnectionPool::Multi does not provide
# friendly API for #checkin or #checkout.  Just use #with.

class ConnectionPool::Multi

  ##
  # The version of connection_pool-multi you are using.

  VERSION = '1.0'

  ##
  # Creates a new connection pool that will create new connections using the
  # given +block+.
  #
  # Valid +options+ include +size+ for the maximum number of connections
  # created and +timeout+ for the time to wait for a connection when the pool
  # is completely checked out.
  #
  # The connection arguments (from #with) are yielded to the block when a new
  # connection of that type is needed.
  #
  #   http_pool = ConnectionPool::Multi.new 50 do |hostname, port|
  #     http = Net::HTTP.new hostname, port
  #     http.start
  #   end

  def initialize options = {}, &block # :yields: *connection_args
    raise ArgumentError, 'Connection pool requires a block' unless
      block_given?

    @size    = options.fetch :size,    ConnectionPool::DEFAULTS[:size]
    @timeout = options.fetch :timeout, ConnectionPool::DEFAULTS[:timeout]

    @available = ConnectionPool::Multi::TimedStack.new @size, &block
    @key = :"current-#{@available.object_id}"
  end

  ##
  # Checks out or creates a connection matching +connection_args+ from the
  # pool.  In +options+ only a <code>:timeout</code> is supported which
  # overrides the default time to wait while checking out a connection.
  #
  #   pool.with ['www.example', 80] do |connection|
  #     connection.get '/foo'
  #   end

  def with connection_args, options = {}
    options[:connection_args] = connection_args

    conn = checkout options

    begin
      yield conn
    ensure
      checkin
    end
  end

  ##
  # Checks out a connection from the pool.  +options+ include:
  #
  # :connection_args ::
  #   The connection creation arguments for the desired connection.
  # :timeout ::
  #   Overrides the default time to wait for a connection when the pool is
  #   completely checked out.

  def checkout options = {}
    connection_args = options[:connection_args]
    stack           = Thread.current[@key] ||= []

    if stack.empty? then
      options[:timeout] ||= @timeout
      conn = @available.pop options
    else
      conn, connection_args = stack.last
    end

    stack.push [conn, connection_args]

    conn
  end

  ##
  # Checks the last checked-out connection into the pool.

  def checkin
    stack = Thread.current[@key]

    raise ConnectionPool::Error, 'no connections are checked out' if
      !stack || stack.empty?

    conn, connection_args = stack.pop

    @available.push conn, connection_args: connection_args if stack.empty?

    nil
  end

  ##
  # Shuts down all connections in the pool.  When the pool is shut down new
  # connections may not be checked out but existing connections may be used
  # until they are checked in.

  def shutdown &block
    @available.shutdown(&block)
  end

end

require 'connection_pool/multi/timed_stack'
