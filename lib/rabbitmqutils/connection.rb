# responsible for listening to the messages from RabbitMQ
class Rabbitmqutils::Connection

  # use to initialize the connection
  def self.init(logger)
    return if @connection
    @connection = new(logger)
  end

  def self.current
    raise "Please call Rabbitmqutils::Connection.init before calling current" if !@connection
    @connection
  end

  attr_reader :channel

  def initialize(logger)
    @bunny = Bunny.new(ENV.fetch("AMQP_URL"), logger: logger)
    @bunny.start
    at_exit { @bunny.stop }

    @channel = @bunny.create_channel

    # send this worker at most 2 messages at a time
    @channel.prefetch(2)
  end

  def close
    @bunny.close if @bunny
  end

end
