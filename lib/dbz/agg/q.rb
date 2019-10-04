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
