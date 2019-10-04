require 'dbz/agg/version'
require 'kafka'
require 'dbz/agg/msg'
require 'dbz/agg/q'

module Dbz
  class Agg

    attr_reader :queue

    def initialize(addrs: ['kafka:9092'], id: '', topics: [], capacity: Q::CAPACITY, interval: 5)
      kafka = Kafka.new(addrs, client_id: id)
      @consumer = kafka.consumer(group_id: id)
      Array(topics).each{ |t| @consumer.subscribe(t) }
      @queue = Q.new(capacity)
      @interval = interval
      run
    end

    def run
      puller
    end

    def puller
      @puller ||= Thread.new do
        @consumer.each_message do |msg|
          @queue.push Msg.new(msg)
        end
      end
    end

    def aggregator
      @aggregator ||= Thread.new do
        loop do
          sleep @interval if @interval
          batch = @queue.batch_shift
          next if batch.empty?
          pp batch.map &:key
        end
      end
    end

  end
end
