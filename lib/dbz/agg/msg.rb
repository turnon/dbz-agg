module Dbz
  class Agg
    class Msg
      def initialize(msg)
        @msg = msg
      end

      def key
        @key ||= "#{table}-#{id}"
      end

      def table
        @table ||= payload['source']['table']
      end

      def id
        @id ||= JSON.parse(@msg.key)['payload']['id']
      end

      def payload
        @payload ||= JSON.parse(@msg.value)['payload']
      end

      def changed_fields
        return @changed_fields if @changed_fields
        before, after = payload['before'], payload['after']
        before.each_pair{ |k, v| @changed_fields << k if after[k] != v }
        @changed_fields
      end

      def op
        @op ||= payload['op']
      end
    end
  end
end
