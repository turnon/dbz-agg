module Dbz
  class Agg
    class Q

      CAPACITY = 5

      def initialize(cap)
        @arr, @cap, @lock, @cond = [], cap, Mutex.new, ConditionVariable.new
      end

      def push(e)
        @lock.synchronize do
          @cond.wait(@lock) while @arr.size >= @cap
          @arr.push(e)
          @cond.signal
        end
      end

      def shift
        e = nil
        deq{ e = @arr.shift }
        e
      end

      def batch_shift(max: CAPACITY)
        b = []
        deq{ b << @arr.shift until @arr.empty? || b.size >= max }
        b
      end

      def group_shift(max: CAPACITY)
        group = Hash.new{ |h, k| h[k] = [] }

        deq do
          until @arr.empty? || group.size >= max
            msg = @arr.shift
            group[msg.key] << msg
          end
        end

        group
      end

      private

      def deq
        @lock.synchronize do
          @cond.wait(@lock) while @arr.empty?
          yield
          @cond.signal
        end
      end
    end
  end
end
