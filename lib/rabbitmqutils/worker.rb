class Rabbitmqutils::Worker
  attr_reader :delivery_info, :properties, :payload
  def initialize(delivery_info, properties, payload)
    @delivery_info, @properties, @payload = delivery_info, properties, payload
  end

  def process
    raise "process method is not implemented on #{self.class}."
  end
end

