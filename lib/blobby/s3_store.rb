require "aws-sdk-resources"
require "blobby"
require "blobby/key_transforming_store"
require "uri"

module Blobby

  # A BLOB store backed by an S3 bucket.
  #
  class S3Store

    def self.from_uri(uri)
      uri = URI(uri)
      raise ArgumentError, "invalid S3 address: #{uri}" unless uri.scheme == "s3"
      bucket_name = uri.host
      prefix = uri.path.sub(%r{\A/}, "").sub(%r{/\Z}, "")
      raise ArgumentError, "no bucket specified" if bucket_name.nil?
      store = new(bucket_name)
      unless prefix.empty?
        store = KeyTransformingStore.new(store) { |key| prefix + "/" + key }
      end
      store
    end

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

      def read
        if block_given?
          s3_object.get do |chunk|
            yield force_binary(chunk)
          end
          nil
        else
          force_binary(s3_object.get.body.read)
        end
      rescue Aws::S3::Errors::NoSuchKey
        nil
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
      @s3_client ||= ::Aws::S3::Client.new(s3_options)
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

  register_store_factory "s3", S3Store

end
