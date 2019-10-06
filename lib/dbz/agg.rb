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
      @capacity = capacity
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
          group = @queue.group_shift(max: @capacity)
          next if group.empty?
          pp group.map{ |k, v| [k, v.map(&:op)]}
        end
      end
    end

    def aggregate
      updated = Hash.new{ |hash, table| hash[table] = {} }
      created, deleted = 2.times.map{ Hash.new{ |hash, table| hash[table] = [] } }

      @queue.group_shift(max: @capacity).each_pair do |key, msgs|
        first = msgs[0]
        table, id = first.table, first.id

        if msgs.count == 1
          if first.create?
            next created[table] << id
          elsif first.delete?
            next deleted[table] << id
          else
            next updated[table][id] = first.changed_fields
          end
        end

        last = msgs[-1]
        if last.delete?
          next if first.create?
          next deleted[table] << id
        end

        next created[table] << id if first.create?

        updated[table][id] = first.fields_changed_after(last)
      end

      {
        created: created,
        updated: updated,
        deleted: deleted
      }
    end

  end
end
