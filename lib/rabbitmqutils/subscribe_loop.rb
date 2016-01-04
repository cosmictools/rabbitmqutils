# responsible for listening to the messages from RabbitMQ
class Rabbitmqutils::SubscribeLoop

  attr_reader :logger, :worker_class, :channel
  def initialize(worker_class, logger = Logger.new(STDOUT))
    @worker_class = worker_class
    @logger = logger
    @connection = Rabbitmqutils::Connection.init(logger)
    @channel = @connection.channel
  end

  def start(queue_name, exchange_name)
    logger.info("> starting subscribe loop for q:#{queue_name}, x:#{exchange_name}")
    exchange = channel.topic(exchange_name, durable: true)

    # bind to the right queue
    queue = channel.queue(queue_name, durable: true).bind(exchange, :routing_key => queue_name)

    # loop
    logger.info("> starting loop")
    queue.subscribe(block: true, manual_ack: true) do |delivery_info, properties, payload|
      begin
        logger.info "> processing #{delivery_info.consumer_tag}"
        worker_class.new(delivery_info, properties, payload).process
        channel.ack(delivery_info.delivery_tag, false)
        logger.info "> done processing #{delivery_info.consumer_tag}"
      rescue => ex
        channel.ack(delivery_info.delivery_tag, false)
        logger.error "> ERROR-ACKED"
        logger.error "> ERROR processing #{delivery_info.consumer_tag}: PAYLOAD: #{payload}"
        logger.error ex
        logger.error(ex.backtrace.join("\n")) if ex.respond_to?(:backtrace)
      end
    end
  rescue Interrupt
    logger.info("> received an interrupt, shutting down.")
  ensure
    puts "> closing connection"
    @connection.close if @connection
  end

end

