module Dbz
  class Agg
    class Q
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
        @lock.synchronize do
          @cond.wait(@lock) while @arr.empty?
          e = @arr.shift
          @cond.signal
        end
        e
      end
    end
  end
end
