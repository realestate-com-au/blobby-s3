require "aws-sdk-v1"
require "forwardable"
require "blobby/key_constraint"

AWS.eager_autoload!(AWS::Core)
AWS.eager_autoload!(AWS::S3)

module Blobby

  # A BLOB store backed by an S3 bucket.
  #
  class S3Store

    # Create a new instance.
    #
    # bucket_name  - name of the bucket to store things in
    # object_acl   - a canned access control policy
    # s3_options   - options passed to AWS::S3.new
    #
    def initialize(bucket_name, object_acl = :private, s3_options = {})
      @bucket_name = bucket_name.to_str
      @s3_options = s3_options.dup
      @s3_options.freeze
      @object_acl = object_acl || :private  # Reset to AWS default if explicitly set to nil
    end

    attr_reader :bucket_name
    attr_reader :s3_options

    def available?
      bucket.objects.first
      true
    rescue ::AWS::Errors::Base
      false
    end

    def [](key)
      KeyConstraint.must_allow!(key)
      StoredObject.new(bucket.objects[key], object_acl)
    end

    class StoredObject

      def initialize(s3_object, acl)
        @s3_object = s3_object
        @acl = acl
      end

      def exists?
        s3_object.exists?
      end

      def read(&block)
        return nil unless s3_object.exists?
        if block_given?
          s3_object.read(&block)
          nil
        else
          s3_object.read
        end
      end

      def write(payload)
        s3_object.write(payload, :acl => acl)
        nil
      end

      def delete
        return false unless s3_object.exists?
        s3_object.delete
        true
      end

      private

      attr_reader :s3_object
      attr_reader :logger
      attr_reader :acl

    end

    private

    def s3_client
      defaults = {
        :s3_endpoint => "s3.amazonaws.com"
      }
      ::AWS::S3.new(defaults.merge(s3_options))
    end

    def bucket
      s3_client.buckets[bucket_name]
    end

    attr_reader :logger
    attr_reader :object_acl

  end

end
