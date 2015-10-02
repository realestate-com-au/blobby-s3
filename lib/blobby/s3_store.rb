require "aws-sdk-resources"
require "forwardable"
require "blobby/key_constraint"

module Blobby

  # A BLOB store backed by an S3 bucket.
  #
  class S3Store

    # Create a new instance.
    #
    # bucket_name  - name of the bucket to store things in
    # s3_options   - options passed to AWS::S3.new
    #
    def initialize(bucket_name, s3_options = {})
      @bucket_name = bucket_name.to_str
      @s3_options = s3_options.dup
      @s3_options[:endpoint] = s3_endpoint_for_bucket
    end

    attr_reader :bucket_name
    attr_reader :s3_options

    def available?
      bucket.objects.first
      true
    rescue ::Aws::Errors::ServiceError
      false
    end

    def [](key)
      KeyConstraint.must_allow!(key)
      StoredObject.new(bucket.object(key))
    end

    class StoredObject

      def initialize(s3_object)
        @s3_object = s3_object
      end

      def exists?
        s3_object.exists?
      end

      def read(&block)
        return nil unless s3_object.exists?
        body = s3_object.get.body
        if block_given?
          body.each_line do |line|
            yield force_binary(line)
          end
          nil
        else
          force_binary(body.read)
        end
      end

      def write(payload)
        s3_object.put(:body => force_binary(payload))
        nil
      end

      def delete
        return false unless s3_object.exists?
        s3_object.delete
        true
      end

      private

      attr_reader :s3_object

      def force_binary(s)
        return s unless s.respond_to?(:encoding)
        return s if s.encoding.name == "ASCII-8BIT"
        s.dup.force_encoding("ASCII-8BIT")
      end

    end

    private

    def s3_client
      ::Aws::S3::Client.new(s3_options)
    end

    def s3_endpoint_for_bucket
      location = s3_client.get_bucket_location(:bucket => bucket_name).location_constraint
      case location
      when ""
        "https://s3.amazonaws.com"
      when "EU"
        "https://s3-eu-west-1.amazonaws.com"
      else
        "https://s3-#{location}.amazonaws.com"
      end
    rescue ::Aws::Errors::ServiceError
      "https://s3.amazonaws.com"
    end

    def s3_resource
      ::Aws::S3::Resource.new(s3_options)
    end

    def bucket
      s3_resource.bucket(bucket_name)
    end

  end

end
