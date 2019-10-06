module Dbz
  class Agg
    class Msg

      TABLE_REGEX = /.*\.(.*)\.Key/

      def initialize(msg)
        @msg = msg
      end

      def key
        @key ||= "#{table}-#{id}"
      end

      def table
        @table ||= decoded_key['schema']['name'].sub(TABLE_REGEX, '\1')
      end

      def id
        @id ||= decoded_key['payload']['id']
      end

      def decoded_key
        @decoded_key ||= JSON.parse(@msg.key)
      end

      def payload
        return @payload if defined? @payload
        @payload = @msg.value ? JSON.parse(@msg.value)['payload'] : nil
      end

      def changed_fields
        return @changed_fields if @changed_fields
        @changed_fields = _diff_fields(payload['before'], payload['after'])
      end

      def fields_changed_after(another)
        _diff_fields(payload['before'], another.payload['after'])
      end

      def create?
        payload && (op == 'c' || op == 'r')
      end

      def update?
        payload && op == 'u'
      end

      def delete?
        payload.nil? || op == 'd'
      end

      def op
        @op ||= payload['op']
      end

      private

      def _diff_fields(before, after)
        [].tap do |fields|
          before.each_pair{ |k, v| fields << k if after[k] != v }
        end
      end
    end
  end
end
